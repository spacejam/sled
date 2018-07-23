use std::fmt;

use super::*;

#[derive(Clone)]
pub struct Acceptor<S>
where
    S: Clone,
{
    store: S,
}

impl<S> fmt::Debug for Acceptor<S>
where
    S: Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Acceptor {{}}",)
    }
}

impl<S> Default for Acceptor<S>
where
    S: Clone + Default,
{
    fn default() -> Acceptor<S> {
        Acceptor {
            store: S::default(),
        }
    }
}

impl<S> Reactor for Acceptor<S>
where
    S: storage::Storage + Clone + Sized,
{
    type Peer = String;
    type Message = Rpc;

    fn receive(
        &mut self,
        _at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match msg {
            ProposeReq(ballot, key) => {
                let current_ballot =
                    self.store.get_highest_seen(key.clone());

                if ballot > current_ballot {
                    self.store.set_highest_seen(
                        key.clone(),
                        ballot.clone(),
                    );
                    vec![(
                        from,
                        ProposeRes {
                            req_ballot: ballot,
                            last_accepted_ballot: self.store
                                .get_accepted_ballot(key.clone()),
                            last_accepted_value: self.store
                                .get_accepted_value(key.clone()),
                            res: Ok(()),
                        },
                    )]
                } else {
                    vec![(
                        from,
                        ProposeRes {
                            req_ballot: ballot,
                            last_accepted_ballot: self.store
                                .get_accepted_ballot(key.clone()),
                            last_accepted_value: self.store
                                .get_accepted_value(key.clone()),
                            res: Err(Error::ProposalRejected {
                                last: current_ballot,
                            }),
                        },
                    )]
                }
            }
            AcceptReq(ballot, key, to) => {
                let current_ballot =
                    self.store.get_highest_seen(key.clone());
                if ballot >= current_ballot {
                    self.store.set_highest_seen(
                        key.clone(),
                        ballot.clone(),
                    );
                    self.store.set_accepted_ballot(
                        key.clone(),
                        ballot.clone(),
                    );
                    self.store.set_accepted_value(key.clone(), to);
                    vec![(from, AcceptRes(ballot, Ok(())))]
                } else {
                    vec![(
                        from,
                        AcceptRes(
                            ballot,
                            Err(Error::AcceptRejected {
                                last: current_ballot,
                            }),
                        ),
                    )]
                }
            }
            _ => panic!("Acceptor got non-propose/accept"),
        }
    }
}
