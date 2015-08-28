# DukeDB

Databasse abstraction system/ORM for the GO language.
The project aims to provide a unified interface to access different underlying database systems.

Right now, only [GORM](http://github.com/jinzhu/gorm) is supported.
It might seem weird to wrap an ORM around another ORM, but 
this abstraction is needed in the [Appkit](http://github.com/theduke/go-appkit) project, 
which provides a go framework for building web applications and APIs.

*In memory, Redis and MongoDB support is in progress.*

## Warning

This project is still under heavy development.
Use with caution.

## License

This project is under the MIT License.
For Details, see LICENSE.txt
