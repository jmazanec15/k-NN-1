import numpy as np
import h5py

TRAIN_NUM_VECTORS = 1000000
TRAIN_DIMENSION = 16000
TEST_NUM_VECTORS = 10000
TEST_DIMENSION = 16000

UPPER_BOUND = 10000.0
LOWER_BOUND = -10000.0

FILE_NAME = "random_16k.hdf5"


def create_data_set(name, hdf5_file, num_vectors, dimension, upper_bound, lower_bound):
    data = np.random.uniform(lower_bound, upper_bound, size=(num_vectors, dimension))
    hdf5_file.create_dataset(name, data=data)


def main():
    # First create file for data set
    f = h5py.File(FILE_NAME, 'w')

    # Next, build HDF5 data set
    create_data_set("train", f, TRAIN_NUM_VECTORS, TRAIN_DIMENSION, UPPER_BOUND, LOWER_BOUND)
    create_data_set("test", f, TEST_NUM_VECTORS, TEST_DIMENSION, UPPER_BOUND, LOWER_BOUND)


if __name__ == "__main__":
    main()
