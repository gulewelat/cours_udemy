# Data visualisation - matplotlib

# Sommaire

# Matplotlib Part 1

# Import

import matplotlib.pyplot as plt
%matplotlib inline

# Matplotlib Part 1

# FUNCTIONAL
plt.plot(x,y)
plt.show() # only when not using jupyter notebook, it prints the plot

plt.plot(x,y, 'r-')
plt.xlabel('X Label')
plt.ylabel('Y Label')
plt.title('Title')
plt.show()

plt.subplot(1, 2, 1) # number of rows, nb of columns, plot you are reffering to
plt.plot(x,y,'r')

plt.subplot(1,2,2)
plt.plot(x,y,'b')

# OO
fig = plt.figure()

axes = fig.add_axes([0.1, 0.1, 0.8, 0.8]) # set axes, left, bottom, width, height

axes.plot(x,y)
axes.set_xlabel('X Label')
axes.set_ylabel('Y Label')
axes.set_title('Set Title')

fig = plt.figure()

axes1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])
axes2 = fig.add_axes([0.2, 0.5, 0.4, 0.3])





