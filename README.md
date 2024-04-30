# Graph Convolutional Matrix Completion

An extension of the Tensorflow based implemention of Graph Convolutional Matrix Completion for recommender systems from the paper:

Rianne van den Berg, Thomas N. Kipf, Max Welling, [Graph Convolutional Matrix Completion](https://arxiv.org/abs/1706.02263) (2017)

Implementation now supports recommendations with Yelp data.


## Requirements

  * Python 2.7
  * TensorFlow (1.4)
  * pandas


## Usage

To reproduce the Yelp recommendation experiments you can run the following commands after cd'ing into the gcmc folder:


**Yelp Philadelphia w/o features**
```bash
python2 train.py -yelp_phil --data_seed 1234 --accum sum -do 0.7 -nsym -nb 2 -e 3500 --testing
```

**Yelp Philadelphia w/ features**
```bash
python2 train.py -yelp_phil --accum stack -do 0.7 -nleft -nb 2 -e 1000 --features --feat_hidden 10 --testing
```

**Yelp Tampa w/o features**
```bash
python2 train.py -yelp_tampa --data_seed 1234 --accum sum -do 0.7 -nsym -nb 2 -e 3500 --testing
```

**Yelp Tampa w/ features**
```bash
python2 train.py -yelp_tampa --accum stack -do 0.7 -nleft -nb 2 -e 1000 --features --feat_hidden 10 --testing
```

## Extra Infomration

We have also included the code we used to generate the Yelp datasets for Philadelphia and Tampa. This code is in the file:
    Yelp_Recommendation_Datasets.py
