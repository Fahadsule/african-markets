import afrimarkets as af

t1=af.get_daily_price("NAE",'CG','all','max')



t2=af.get_index_price

t3=af.get_rights_data("DSE","TCL","all","max")

t4=af.get_ticker_changes("NASE")
print(t4)