from preprocessing import load_data_monti
from preprocessing import load_matlab_file

out = load_data_monti('flixster')

# import numpy as np
# import h5py
# f = h5py.File('./data/flixster/training_test_dataset.mat','r')
# data = f["Otraining"]
# data = np.array(data)
# print(data)