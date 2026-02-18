'''This file will store code to take data and convert it into graphs and charts'''

import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
from datetime import datetime, timedelta


#Function below generates a bar graph that shows the fire distribution by month
def graph_fire_count_by_month(rows):
    month_list = list()
    fire_count_list = list()
    month_mappings = {1.0: "Jan", 2.0: "Feb", 3.0: "Mar", 4.0: "Apr", 5.0: "May", 6.0: "Jun", 7.0: "Jul", 8.0: "Aug", 9.0: "Sep", 10.0: "Oct", 11.0: "Nov", 12.0: "Dec" }
    
    for index in range(len(rows)):
        month_list.append(month_mappings[rows[index][0]])
        fire_count_list.append(rows[index][1])

    plt.bar(month_list, fire_count_list, zorder=3)
    plt.title("Number of Fires by Month in the 2010s")
    plt.ylabel("Number of fires")
    plt.xlabel("Months")
    plt.grid(True)
    plt.grid(zorder=0)

    plt.savefig("./src/static/images/fire_count_by_month.png")
    plt.show()

#Function below generates a heat map of the United States, showing where fires occured, and how severe they were
def plot_usa_heatmap(rows):
    latitude_list = list()
    longitude_list = list()
    size_list = list()
    #This dict stores new mappings for size class, which will allow size of fire to be accounted for
    # size_weight = {"A": .001, "B": .01, "C": .3, "D": .4, "E": .5, "F": .8, "G": 1}
    size_weight = {"A": .125, "B": 5.2, "C": 55, "D": 200, "E": 650, "F": 3000, "G": 500000}
    for index in range(len(rows)):
        longitude_list.append(rows[index][0])
        latitude_list.append(rows[index][1])
        size_list.append(size_weight[rows[index][3]])

    us_map = folium.Map(location=[39.0119, -98.4842], zoom_start = 5)

    heatmap = HeatMap(list(zip(latitude_list, longitude_list, size_list)), min_opacity=0, max_opacity=1.0, radius=30, blur=20, max_zoom=1)

    heatmap.add_to(us_map)

    us_map.save("./src/static/html/fire_heat_map.html")


# This function displays a pie chart which shows what fire types are most prevalent. 
# The chart uses all states and their top fire causes to make this pie chart
def graph_state_top_causes(rows):
    label_list = list()
    count_list = list()
    for index in range(len(rows)):
        label_list.append(rows[index][0])
        count_list.append(rows[index][1])

    plt.pie(count_list, labels=label_list)
    plt.title("Top Causes from Every State")

    plt.savefig("./src/static/images/top_causes_from_every_state.png")
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

    plt.figure(figsize=(18, 10))
    for current_subplot in range(4):

        plt.subplot(2, 2, current_subplot + 1)
        plt.rcParams['figure.constrained_layout.use'] = True
        cause_list = list()
        count_list = list()

        subplot_cause_rows = cause_rows[current_subplot]
        subplot_total_fires = total_fires[current_subplot]
        count_list.append(subplot_total_fires[0][0])
        cause_list.append('All fires')
        for index in range(len(cause_rows)):
            cause_list.append(subplot_cause_rows[index][0])
            count_list.append(subplot_cause_rows[index][1])
        plt.bar(cause_list, count_list, zorder=3)
        plt.grid(True)
        plt.title(f"Number of Fires Over {min_acreage[current_subplot]} Acres by Type")
        plt.xlabel("Fire types")
        plt.ylabel("Number of fires")

    plt.autoscale(True)
    plt.savefig("./src/static/images/top_causes_subplot.png")
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

def graph_states_with_highest_acreage_sums(rows):
    label_list = list()
    acreage_list = list()

    for x, y in rows:
        label_list.append(x)
        acreage_list.append(int(float(y)/1000000))
    
    plt.bar(label_list, acreage_list, zorder=2)
    plt.title('Ten States With the Most Burned Acreage')
    plt.xlabel('State')
    plt.ylabel('Acres in millions')
    plt.grid(True, zorder=0)
    plt.savefig("./src/static/images/states_with_highest_acreage_sums.png")
    plt.show()