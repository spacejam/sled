# art

concurrent adaptive radix tree

```
use art::Art;

let mut a = Art::default();

a.insert(b"some key", b"some value");

assert_eq!(a.get(b"some key"), Some(b"some value"));
```

#### references
[The ART of Practical Synchronization](https://db.in.tum.de/~leis/papers/artsync.pdf)
