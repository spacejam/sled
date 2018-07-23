use super::*;

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
enum Act {
    Publish(Option<Vec<u8>>),
    Observe(Option<Vec<u8>>),
    Consume(Option<Vec<u8>>),
}

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct Event {
    at: SystemTime,
    act: Act,
    client_req_id: u64,
}

// simple (simplistic, not exhaustive) linearizability checker:
// main properties:
//   1. an effect must NOT be observed before
//      its causal operation starts
//   2. after its causal operation ends,
//      an effect MUST be observed
//
// for each successful operation end time
//   * populate a pending set for possibly
//     successful operations that start before
//     then (one pass ever, iterate along-side
//     the end times by start times)
//   * for each successful operation that reads
//     a previous write, ensure that it is present
//     in the write set.
//   * for each successful operation that "consumes"
//     a previous write (CAS, Del) we try to pop
//     its consumed value out of our pending set
//     after filling the pending set with any write
//     that could have happened before then.
//     if its not there, we failed linearizability.
//
fn check_linearizability(
    request_rpcs: Vec<ScheduledMessage>,
    response_rpcs: Vec<ScheduledMessage>,
) -> bool {
    use Req::*;
    // publishes "happen" as soon as a not-explicitly-failed
    // request begins
    let mut publishes = vec![];
    // observes "happen" at the end of a succesful response or
    // cas failure
    let mut observes = vec![];
    // consumes "happen" at the end of a successful response
    let mut consumes = vec![];

    let responses: std::collections::BTreeMap<_, _> = response_rpcs
        .into_iter()
        .filter_map(|r| {
            if let Rpc::ClientResponse(id, res) = r.msg {
                Some((id, (r.at, res)))
            } else {
                panic!("non-ClientResponse sent to client")
            }
        })
        .collect();

    for r in request_rpcs {
        let (id, req) = if let Rpc::ClientRequest(id, req) = r.msg {
            (id, req)
        } else {
            panic!("Cluster started with non-ClientRequest")
        };

        let begin = r.at;

        //  reasoning about effects:
        //
        //  OP  | res  | consumes | publishes | observes
        // -----------------------------------------
        //  CAS | ok   | old      | new       | old
        //  CAS | casf | -        | -         | actual
        //  CAS | ?    | ?        | ?         | ?
        //  DEL | ok   | ?        | None      | -
        //  DEL | ?    | ?        | None?     | -
        //  SET | ok   | ?        | value     | -
        //  SET | ?    | ?        | value?    | -
        //  GET | ok   | -        | -         | value
        //  GET | ?    | -        | -         | value?
        match responses.get(&id) {
            None |
            Some(&(_, Err(Error::Timeout))) |
            Some(&(_,
                   Err(Error::AcceptRejected {
                           ..
                       }))) => {
                // not sure if this actually took effect or not.
                // NB this is sort of weird, because even if an accept was
                // rejected by a majority, it may be used as a later message.
                // so as a client we have to treat it as being in a weird pending
                // state.
                match req {
                    Cas(k, _old, new) => publishes.push((k, begin, new, id)),
                    Del(k) => publishes.push((k, begin, None, id)),
                    Set(k, value) => {
                        publishes.push((k, begin, Some(value), id))
                    }
                    Get(_k) => {}
                }
            }
            Some(&(end, Ok(ref v))) => {
                match req {
                    Cas(k, old, new) => {
                        consumes.push((k.clone(), end, old.clone(), id));
                        observes.push((k.clone(), end, old, id));
                        publishes.push((k.clone(), begin, new, id));
                    }
                    Get(k) => observes.push((k, end, v.clone(), id)),
                    Del(k) => publishes.push((k, end, None, id)),
                    Set(k, value) => publishes.push((k, end, Some(value), id)),
                }
            }
            Some(&(end, Err(Error::CasFailed(ref witnessed)))) => {
                match req {
                    Cas(k, _old, _new) => {
                        observes.push((k, end, witnessed.clone(), id));
                    }
                    _ => panic!("non-cas request found for CasFailed response"),
                }
            }
            _ => {
                // propose/accept failure, no actionable info can be derived
            }
        }
    }

    let mut events_per_k = HashMap::new();

    for (k, time, value, id) in publishes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Publish(value),
            client_req_id: id,
        });
    }

    for (k, time, value, id) in consumes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Consume(value),
            client_req_id: id,
        });
    }

    for (k, time, value, id) in observes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Observe(value),
            client_req_id: id,
        });
    }

    for (_k, mut events) in events_per_k.into_iter() {
        events.sort();

        let mut value_pool = HashMap::new();
        value_pool.insert(None, 1);

        for event in events {
            // println!("k: {:?}, event: {:?}", _k, event);
            match event.act {
                Act::Publish(v) => {
                    let mut entry = value_pool.entry(v).or_insert(0);
                    *entry += 1;
                }
                Act::Observe(v) => {
                    let count = value_pool.get(&v).unwrap();
                    assert!(
                        *count > 0,
                        "expect to be able to witness {:?} at time {:?} for req {}",
                        v,
                        event.at,
                        event.client_req_id
                    )
                }
                Act::Consume(v) => {
                    let mut count = value_pool.get_mut(&v).unwrap();
                    assert!(*count > 0);
                    *count -= 1;
                }
            }
        }
    }

    true
}
