# Patent Data Investigation

## Overview

Investigation into the different possible patent data sources for ai_gemonics.

| Data Source   | Summary       | How to Access (format, restrictions) | Timeliness 
| ------------- |:-------------:| -----:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |

## Patent Lens

- need to pass a query.
- example query:

```
data = '''{
              "query": {
                  "terms":  {
                      "lens_id": ["031-156-664-516-153"]
                  }
              },
              "include": ["biblio", "doc_key"]
}'''
```
