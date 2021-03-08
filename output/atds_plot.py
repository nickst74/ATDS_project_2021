# https://matplotlib.org/stable/gallery/lines_bars_and_markers/barchart.html
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


labels = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
rdd_time = [12.133, 49.684, 57.143, 13.338, 371.662]
sql_csv_time = [36.618, 54.457, 58.062, 27.242, 107.160]
sql_par_time = [21.381, 21.331, 26.855, 23.005, 64.023]

x = np.arange(len(labels))  # the label locations
width = 0.25  # the width of the bars

fig, ax = plt.subplots(figsize=(15,10))
rdd = ax.bar(x - width, rdd_time, width, label='rdd')
sql_csv = ax.bar(x, sql_csv_time, width, label='sql_csv')
sql_par = ax.bar(x + width, sql_par_time, width, label='sql_par')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (sec)')
ax.set_title('Execution time of implementations (grouped by Query)')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rdd)
autolabel(sql_csv)
autolabel(sql_par)

fig.tight_layout()

plt.show()