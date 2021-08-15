# Instructions

## Setting up

```zsh
# convert the data to the right formats
python main.py initiate

python main.py training

python main.py make-indexes
```


## Serving

```
uvicorn main:app
```

## Adding a new day

```zsh
python main.py date-increment
```

at this stage, take a look at the pdf export to see if it looks right.
Then:


```zsh
python main.py training
python main.py make-indexes
```


----


## Dataset

Dataset is amazon reviews dataset for Musical Instruments.
