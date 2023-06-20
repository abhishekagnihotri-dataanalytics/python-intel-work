import pandas as pd
# df = pd.read_excel(r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\Sustainability\Ariba\ExecDash_Ariba_Contract.xls", engine='xlrd', header=5)
# df = pd.read_excel(r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\test\Zano.xls",engine='xlrd', header=5)
df = pd.read_html(r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmbi\Sustainability\Ariba\ExecDash_Ariba_Contract.xls")[-1]
# print(df)
print(df[-1])
