# coding: utf-8

import numpy as np

if __name__ == "__main__":
    path = u'/tmp/spark/preparert/part-00000'
    np.set_printoptions(suppress=True)
    data = np.loadtxt(path, dtype=float, delimiter='\t')
    # print type(data)
    y,x = np.split(data,(6,))
    print x
    print y[5:6]
