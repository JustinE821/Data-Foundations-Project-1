import pandas as pd

df = pd.read_csv('./resources/csv_files/New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv')
#print("df creation successful")

trimmed_df = df[["Fire Number", "Incident Name", "Fire Start Date", "Fire Out Date", "County", "Municipality", "Cause"]]


fire_cause = trimmed_df.groupby('Cause')["Cause"].count()
print(trimmed_df.groupby('Cause')["Cause"].count())

#Values commented out show how few deaths occur from the wildfires in NY
#fatal_fires = df[df["Fatalities"] > 0]
#print(fatal_fires[["Fire Number", "Incident Name", "Fire Start Date", "Fire Out Date", "County", "Municipality", "Fatalities"]])

print(trimmed_df.head(20))
