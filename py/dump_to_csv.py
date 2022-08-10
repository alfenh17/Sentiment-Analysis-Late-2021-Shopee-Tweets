# Import mysql dan pandas
import mysql.connector as mysql
import pandas as pd

# Menghubungkan ke mysql melalui localhost
database = mysql.connect(
    host = 'localhost',
    user = 'root',
    passwd = '',
    database = "shop"
)

cursor = database.cursor()

# Proses query kolom id_user, cuitan dan tanggal cuit dari tabel shopee dari database shopeee
select = "select id_user ,cuitan ,tanggal_cuit from shopee"

# Mengambil text
dataBase = cursor.execute(select)
records = cursor.fetchall()
data = pd.DataFrame(records)

data.to_csv("2021_shopee_tweet.csv", header=["id_user", "shopee_tweet", "tweet_date"], index=False)