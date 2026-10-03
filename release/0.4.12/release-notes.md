# SPLime 0.4.12

This release includes signed current-process Public Object execution with native
Python inputs and results, including pandas DataFrames through `run.value`.
Existing public releases retain their signed execution profile.

It also fixes Console archive integrity for the current browser bootstrap and
Windows embedded-cache binary I/O and read-only cleanup. Built Console archives
are exercised through their actual release gate before deployment.

The 0.4.11 source tag remains immutable. Its failing Console archive was not
deployed; 0.4.12 supersedes that candidate. PyPI and Docker publication are
separate release steps and are not implied by a server deployment.

Platform scope: current-process calls support Linux, macOS and Windows on
CPython 3.13. Legacy isolated workers retain their documented POSIX-only timeout
contract; Windows Job Object containment is not part of this release.
