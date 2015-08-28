# DukeDB

Databasse abstraction system/ORM for the GO language.
The project aims to provide a unified interface to access different underlying database systems.

Right now, only GORM is supported.
It might seem weird to wrap an ORM around another ORM, but 
this abstraction is needed in the Appkit project, which provides a go framework 
for building web applications and APIs.

In memory, Redis and MongoDB support is in progress.


## License

This project is under the MIT License.
For Details, see LICENSE.txt

