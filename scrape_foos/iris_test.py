import pandas as pd
data = [['Alex',10],['Bob',12],['Clarke',13]]
df = pd.DataFrame(data,columns=['Name','Age'])
df.to_gbq(project_id='scarlet-labs', destination_table='testing.iris_test', if_exists="replace")
