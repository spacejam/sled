use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::fs::OpenOptions;

use libc::dup2;

use rand;
use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};

use voidmap::*;

use std::fmt;

#[derive(Clone)]
enum Op {
    Set(Key, Value),
    Del(Key),
    Cas(Key, Value, Value),
    Split(PageID),
    Merge(PageID),
    Consolidate(PageID),
}

impl fmt::Debug for Op {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt::Debug::fmt(&self.event, formatter)
    }
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Op {
        let (c, u, x, y) =
            (g.gen_ascii_chars().nth(0).unwrap(), g.gen::<char>(), g.gen::<u16>(), g.gen::<u16>());
        let events = vec![
                Event::Mouse(MouseEvent::Release(x, y)),
            ];
        Op { event: g.choose(&*events).unwrap().clone() }
    }
}

#[derive(Debug, Clone)]
struct History(Vec<Op>);

impl Arbitrary for History {
    fn arbitrary<G: Gen>(g: &mut G) -> History {
        let commands = vec![
            "kontent",
        ];
        let mut choice = vec![];

        for _ in 0..g.gen_range(0, 2) {
            let command = g.choose(&*commands).unwrap().clone();
            let mut chars = command.chars().collect();
            choice.append(&mut chars);
            if g.gen_range(0, 10) > 0 {
                choice.push(' ');
            }
        }


        History(choice.into_iter()
            .map(|c| Op { event: Event::Key(Key::Char(c)) })
            .collect())
    }
}


#[derive(Debug, Clone)]
struct OpVec {
    ops: Vec<Op>,
}

impl Arbitrary for OpVec {
    fn arbitrary<G: Gen>(g: &mut G) -> OpVec {
        let mut ops = vec![];
        for _ in 0..g.gen_range(1, 100) {
            if g.gen_range(0, 10) > 0 {
                ops.push(Op::arbitrary(g));
            } else {
                let mut content = History::arbitrary(g);
                ops.append(&mut content.0);
            }
        }
        OpVec { ops: ops }
    }

    fn shrink(&self) -> Box<Iterator<Item = OpVec>> {
        let mut smaller = vec![];
        for i in 0..self.ops.len() {
            let mut clone = self.clone();
            clone.ops.remove(i);
            smaller.push(clone);
        }

        Box::new(smaller.into_iter())
    }
}

fn prop_handle_events(ops: OpVec, dims: (u16, u16)) -> bool {
    let mut screen = Screen::default();
    screen.is_test = true;
    screen.start_raw_mode();
    screen.draw();
    screen.dims = dims;

    for op in &ops.ops {
        let should_break = !screen.handle_event(op.event.clone());

        if screen.should_auto_arrange() {
            screen.arrange();
        }

        screen.draw();
        screen.assert_node_consistency();

        if should_break {
            screen.cleanup();
            screen.save();
            break;
        }
    }
    true
}

#[test]
fn qc_input_events_dont_crash_void() {
    // redirect stdout to quickcheck.out to make travis happy
    let f = OpenOptions::new()
        .append(true)
        .create(true)
        .open("quickcheck.out")
        .unwrap();
    let fd = f.as_raw_fd();
    unsafe {
        dup2(fd, 1);
    }

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 1))
        .tests(100_000)
        .max_tests(1_000_000)
        .quickcheck(prop_handle_events as fn(OpVec, (u16, u16)) -> bool);
}

#[derive(Debug)]
struct Runs {
    choices: Vec<usize>,
    on_tail: bool,
}

impl Runs {
    fn new<I>(inner: &Vec<Vec<I>>) -> Runs {
        let choices = inner.iter()
            .enumerate()
            .flat_map(|(i, v)| vec![i; v.len()])
            .collect();

        Runs {
            choices: choices,
            on_tail: false,
        }
    }
}

impl Iterator for Runs {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        // from: "The Art of Computer Programming" by Donald Knuth
        if !self.on_tail {
            self.on_tail = true;
            return Some(self.choices.clone());
        }

        let size = self.choices.len();

        if self.on_tail && size < 2 {
            return None;
        }

        let mut j = size - 2;
        while j > 0 && self.choices[j] >= self.choices[j + 1] {
            j -= 1;
        }

        if self.choices[j] < self.choices[j + 1] {
            let mut l = size - 1;
            while self.choices[j] >= self.choices[l] {
                l -= 1;
            }
            self.choices.swap(j, l);
            self.choices[j + 1..size].reverse();
            Some(self.choices.clone())
        } else {
            None
        }
    }
}

pub struct Process<A>(Vec<(Option<String>, Box<Fn(A) -> A>)>);

impl<A> Process<A> {
    fn new<'a>(steps: Vec<(&'a str, Box<Fn(A) -> A>)>) -> Process<A> {
        Process(steps.into_iter().map(|(m, f)| (Some(m.to_owned()), f)).collect())
    }
}

pub struct System<A> {
    pub state: A,
    pub invariants: Vec<Box<Fn(&A) -> bool>>,
    pub processes: Vec<Vec<(Option<String>, Box<Fn(A) -> A>)>>,
}

impl<A: Clone> System<A> {
    fn test(&self) -> Result<(), String> {
        let runs = Runs::new(&self.processes);
        for run in runs {
            let mut indices = HashMap::new();
            let mut state = self.state.clone();
            for choice in run {
                let index = indices.entry(choice).or_insert(0);
                let (ref note, ref f) = self.processes[choice][*index];
                *index += 1;

                state = f(state);

                for invariant in &self.invariants {
                    if !invariant(&state) {
                        return Err("failed invariant".to_owned());
                    }
                }
            }
        }
        Ok(())
    }
}


fn fact(n: usize) -> usize {
    (1..n + 1).fold(1, |p, n| p * n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let a = vec![11, 12];
        let b = vec![21, 22];
        let c = vec![31, 32];

        let runs = Runs::new(&vec![a, b, c]);
        for i in runs {
            println!("i: {:?}", i);
        }

        let p_a = Process::new(vec![
            ("a1", Box::new(|()| println!("a1"))),
            ("a2", Box::new(|()| println!("a2"))),
            ("a3", Box::new(|()| println!("a3"))),
        ]);

        let p_b = Process::new(vec![
            ("b1", Box::new(|()| println!("b1"))),
            ("b2", Box::new(|()| println!("b2"))),
            ("b3", Box::new(|()| println!("b3"))),
        ]);

        let system = System {
            state: (),
            invariants: vec![],
            processes: vec![p_a.0, p_b.0],
        };

        system.test().unwrap();

    }
}
