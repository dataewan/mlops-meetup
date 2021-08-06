# Instructions

## Setting up

```zsh
# convert the data to the right formats
python main.py initiate

python main.py training

python main.py make_indexes
```


## Serving

```
uvicorn main:app
```

----

Workshop for the mlops meetup.

There are a lot of things you have to consider when developing a machine learning system, including:

- The modelling
- Technology, and _production code_
- The data work

The data work, and how it overlaps with the other components, is often overlooked.
It is less exciting to talk about.
Not as glitzy.
This is changing a bit now, papers like _"Everyone wants to do the model work, not the data work": Data Cascades in High-Stakes AI_ from Google talks about the importance of getting your data right.
Andrew Ng, one of the people responsible for the popularity of this current wave of ML, has been talking about how [moving from model centric to a data centric approach](https://www.youtube.com/watch?v=06-AZXmwHjo) gives more accurate results and allows you to solve "small data" problems.


I'm using a recommender system as an example of a real world type of ML system, trying to show how focussing too much on _production code_ doesn't get you out of problems,
and how including more on the data work can get you out of trouble.

**Notes**

- This is not a tutorial in how to create a recommender system. The patterns, technologies, and data transformations aren't meant to be gold standard. This doesn't look like any particular recommender I've seen before.
- I'm not trying to say focussing on data work is more important than other things you need to consider.


## Dataset

Dataset is amazon reviews dataset for Musical Instruments.
