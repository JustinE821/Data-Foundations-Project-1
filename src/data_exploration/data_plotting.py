'''This file will store code to take data and convert it into graphs and charts'''

import matplotlib.pyplot as plt


# This function displays a pie chart which shows what fire types are most prevalent. 
# The chart uses all states and their top fire causes to make this pie chart
def graph_state_top_causes(rows):
    label_list = list()
    count_list = list()
    for index in range(len(rows)):
        label_list.append(rows[index][0])
        count_list.append(rows[index][1])

    plt.pie(count_list, labels=label_list)
    plt.title("Top causes from every state")
    plt.show()

# This graph technically works, but probably will not be used
def graph_states_by_count(rows):

    label_list = list()
    count_list = list()
    for index in range(len(rows)):
        label_list.append(rows[index][0])
        count_list.append(rows[index][1])

    fig, ax = plt.subplots()
    ax.bar(label_list, count_list)
    plt.show()

# Makes a subplot showing 4 different bar graphs, which show the top 5 causes of fires with a minimum fire size set
def graph_top_causes(cause_rows, total_fires):
    min_acreage = [0, 5, 100, 1000]
    for current_subplot in range(4):
        plt.subplot(2, 2, current_subplot + 1)
        cause_list = list()
        count_list = list()

        subplot_cause_rows = cause_rows[current_subplot]
        subplot_total_fires = total_fires[current_subplot]
        count_list.append(subplot_total_fires[0][0])
        cause_list.append('All fires')
        for index in range(len(cause_rows)):
            cause_list.append(subplot_cause_rows[index][0])
            count_list.append(subplot_cause_rows[index][1])

        plt.bar(cause_list, count_list)
        plt.grid(True)
        plt.title(f"Number of fires over {min_acreage[current_subplot]} acres by type")
        plt.xlabel("Fire types")
        plt.ylabel("Number of fires")

    plt.show()


# Still working on this one, may end up scrapped
def graph_top_fire_cause_by_state(top_cause_rows, total_fires):

    label_list = list()
    cause_type = list()
    count_list = list()

    for index in range(len(top_cause_rows)):
        label_list.append(top_cause_rows[index][0])
        cause_type.append(top_cause_rows[index][1])
        count_list.append(top_cause_rows[index][2])