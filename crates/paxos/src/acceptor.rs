use super::*;

pub struct Acceptor {
    highest_seen: Ballot,
    state: Option<Value>,
}

impl Reactor for Acceptor {
    type Peer = SocketAddr;
    type Message = Rpc;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match msg {
            ProposeReq(ballot) => {
                if ballot > self.highest_seen {
                    self.highest_seen = ballot;
                    return vec![
                        (
                            from,
                            ProposeRes(ballot, Ok(self.state.clone()))
                        ),
                    ];
                } else {
                    return vec![
                        (
                            from,
                            ProposeRes(
                                ballot,
                                Err(Error::ProposalRejected {
                                    last: self.highest_seen.clone(),
                                }),
                            )
                        ),
                    ];
                }
            }
            AcceptReq(ballot, to) => {
                if ballot >= self.highest_seen {
                    self.highest_seen = ballot;
                    self.state = to;
                    return vec![(from, AcceptRes(ballot, Ok(())))];
                } else {
                    return vec![
                        (
                            from,
                            AcceptRes(
                                ballot,
                                Err(Error::AcceptRejected {
                                    last: self.highest_seen.clone(),
                                }),
                            )
                        ),
                    ];
                }
            }
            _ => panic!("Acceptor got non-propose/accept"),
        }
    }
}
