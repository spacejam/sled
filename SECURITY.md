# Security Policy

## Reporting a Vulnerability

sled uses some unsafe functionality in the core lock-free algorithms, and in a few places to more efficiently copy data.

Please contact [Tyler Neely](mailto:tylerneely@gmail.com?subject=sled%20security%20issue) immediately if you find any vulnerability, and I will work with you to fix the issue rapidly and coordinate public disclosure with an expedited release including the fix.

If you are a bug hunter or a person with a security interest, here is my mental model of memory corruption risk in the sled codebase:

1. memory issues relating to the lock-free data structures in their colder failure paths. these have been tested a bit by injecting delays into random places, but this is still an area with elevated risk
1. anywhere the `unsafe` keyword is used
