
# Used car price prediction in Kazakhstan

Projects implements the following:
- parser for kolesa.kz;
- predictive model training code on dataset from kolesa.kz.


### How To Use

Enter full or relative path for file with the data. If a file with the data does not exist, the program will create new dataset and name it with the entered name.

```sh
positional arguments:
  p             path to file with data

optional arguments:
  -h, --help    show this help message and exit
  --update [u]  update dataset
```
if --update is set,  parser will start to update the existing dataset
