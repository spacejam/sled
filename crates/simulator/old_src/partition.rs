use super::*;

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct Partition {
    at: SystemTime,
    duration: Duration,
    from: String,
    to: String,
}

impl Partition {
    fn generate<G: Gen>(
        g: &mut G,
        clients: usize,
        proposers: usize,
        acceptors: usize,
    ) -> Self {
        static NAMES: [&'static str; 3] =
            ["client:", "proposer:", "acceptor:"];

        let from_choice = g.gen_range(0, 3);
        let mut to_choice = g.gen_range(0, 3);

        while to_choice == from_choice {
            to_choice = g.gen_range(0, 3);
        }

        let at =
            UNIX_EPOCH.add(Duration::new(0, g.gen_range(0, 100)));
        let duration = Duration::new(0, g.gen_range(0, 100));

        let mut n = |choice| match choice {
            0 => g.gen_range(0, clients),
            1 => g.gen_range(0, proposers),
            2 => g.gen_range(0, acceptors),
            _ => panic!("too high"),
        };

        let from =
            format!("{}{}", NAMES[from_choice], n(from_choice));
        let to = format!("{}{}", NAMES[to_choice], n(to_choice));
        Partition {
            at: at,
            duration: duration,
            to: to,
            from: from,
        }
    }
}
