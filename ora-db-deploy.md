## Improvements

Always compile derived objects
Make sure schema is usable : check invalid objects (use dependancy aware utilities to recompile?)
To check if a schema has changed : hash data dictionary contents

1. Create tracking tables 
1. Version numbering at schema level (build number? db version number?)
1. Schema changed unexpectidly? ==> Checksum
    1. Bare minimum
        1. objects (dba_objects : too specific to oracle?)
        1. table/view columns
        1. procs (procedures, functions, packages)
    1. to consider
        1. indexes
        1. triggers
        1. types
1. Tracking individual object changes? ==> table comments/column comments?
1. Listing changes


## Kind of changes

* table creation/drop
* column add/drop
* view creation/drop

