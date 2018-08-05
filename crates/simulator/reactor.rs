use super::*;

// Reactor is a trait for building simulable systems.
pub trait Reactor: Debug + Clone {
    type Peer: std::net::ToSocketAddrs;
    type Message: Serialize + DeserializeOwned;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn tick(
        &mut self,
        _at: SystemTime,
    ) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}
