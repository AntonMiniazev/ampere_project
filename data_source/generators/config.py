# Initialization parameters
n_of_init_clients = 4000
n_delivery_resource = 125
database_init = "source"
schema_init = "core"


# Orders constants
n_orders_days = 10

# Client constants
churn_rates = [0.004, 0.007]
new_clients_rates = [0.006, 0.01]


# Constants for order generation
avg_orders = 1.35
min_orders = 1
max_orders = 4

# Constants for product generation
avg_products = 10
min_products = 5
max_products = 20

# Range for product number/weight
min_units = 1
max_units = 5
min_weight = 0.100
max_weight = 4.000

# Statuses for order generation - TODO: Replace with dictionary source
delivery_type_ids = [1, 2, 3]
status_ids = [1, 2, 3]

# Delivery type probabilities
dt_p1 = [0.45, 0.55]
dt_p2 = [0.30, 0.40]

# Order statuses probabilities
os_p1 = [0.005, 0.01]
os_p2 = [0.97, 0.98]

# Order source scaled params:
ord_source_p = [2, 1, 3]

# Order Status History constants
courier_hours = 8
courier_clock_delta = [0, 16]
order_cycle_minutes = [45, 55]
time_between_statuses = [8, 15]
eod_orders_time = 19  # around 7 pm orders are transfered to the next date

# Payment type probabilities
pmt_type_p = [0.6, 0.2, 0.05, 0.15]
prepayment_p = [0.2, 0.8]

# Delivery tracking constants
pickup_timing = [5, 15]
