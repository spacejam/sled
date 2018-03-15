use super::*;

#[derive(Default, Debug, Clone)]
pub struct Acceptor {
    highest_seen: Ballot,
    last_accepted_ballot: Ballot,
    last_accepted_value: Option<Value>,
}

impl Reactor for Acceptor {
    type Peer = String;
    type Message = Rpc;

    fn receive(
        &mut self,
        _at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match msg {
            ProposeReq(ballot) => {
                if ballot > self.highest_seen {
                    self.highest_seen = ballot.clone();
                    vec![
                        (
                            from,
                            ProposeRes {
                                req_ballot: ballot,
                                last_accepted_ballot: self.last_accepted_ballot
                                    .clone(),
                                last_accepted_value: self.last_accepted_value
                                    .clone(),
                                res: Ok(()),
                            }
                        ),
                    ]
                } else {
                    vec![
                        (
                            from,
                            ProposeRes {
                                req_ballot: ballot,
                                last_accepted_ballot: self.last_accepted_ballot
                                    .clone(),
                                last_accepted_value: self.last_accepted_value
                                    .clone(),
                                res: Err(Error::ProposalRejected {
                                    last: self.highest_seen.clone(),
                                }),
                            }
                        ),
                    ]
                }
            }
            AcceptReq(ballot, to) => {
                if ballot >= self.highest_seen {
                    self.highest_seen = ballot.clone();
                    self.last_accepted_ballot = ballot.clone();
                    self.last_accepted_value = to;
                    vec![(from, AcceptRes(ballot, Ok(())))]
                } else {
                    vec![
                        (
                            from,
                            AcceptRes(
                                ballot,
                                Err(Error::AcceptRejected {
                                    last: self.highest_seen.clone(),
                                }),
                            )
                        ),
                    ]
                }
            }
            _ => panic!("Acceptor got non-propose/accept"),
        }
    }
}
