```
    ███╗   ███╗██╗███╗   ██╗██╗ ██████╗ █████╗  ██████╗██╗  ██╗███████╗
    ████╗ ████║██║████╗  ██║██║██╔════╝██╔══██╗██╔════╝██║  ██║██╔════╝
    ██╔████╔██║██║██╔██╗ ██║██║██║     ███████║██║     ███████║█████╗  
    ██║╚██╔╝██║██║██║╚██╗██║██║██║     ██╔══██║██║     ██╔══██║██╔══╝  
    ██║ ╚═╝ ██║██║██║ ╚████║██║╚██████╗██║  ██║╚██████╗██║  ██║███████╗
    ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝ ╚═════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝

```
minicache - In progress implementation for: https://codingchallenges.fyi/challenges/challenge-memcached/

Current features:

* Operations: Set and Get.
* Somewhat proper text protocol, will work with telnet.
* Multiple users concurrency.
* Passive TTL management and a simple implementation for active TTL management.

Things I want to add:
* More operations like prepend and append.
* Per key locks instead of shard level locks.
* Zero-copy when parsing data.
* Better compliance with memcached text prortocol.

🦀 🚀