from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, when

def find_product_category_pairs(products_df):
    filled_df = products_df.withColumn("categories", when(col("categories").isNull(), array()).otherwise(col("categories")))
    exploded_df = filled_df.select("product_name", explode("categories").alias("category"))
    product_category_pairs = exploded_df.select("product_name", "category")
    products_with_no_category = products_df.filter(col("categories").isNull()).select("product_name")
    return product_category_pairs, products_with_no_category

# Создаем сессию Spark
spark = SparkSession.builder \
    .appName("ProductCategoryPairs") \
    .getOrCreate()

# Пример входных данных
data = [("product1", ["category1", "category2", "category3"]),
        ("product2", ["category2", "category3"]),
        ("product3", None),
        ("product4", ["category3", "category4"]),
        ("product5", ["category4", "category5"]),
        ("product6", None),
        ("product7", ["category6", "category7"]),
        ("product8", ["category8"]),
        ("product9", ["category9", "category10"]),
        ("product10", ["category10"])]

# Создаем DataFrame из входных данных
products_df = spark.createDataFrame(data, ["product_name", "categories"])

# Получаем пары "Имя продукта - Название категории" и продукты без категорий
product_category_pairs, products_with_no_category = find_product_category_pairs(products_df)

# Отображаем результаты
print("Пары продукт-категория:")
product_category_pairs.show(truncate=False)

print("Продукты без категорий:")
products_with_no_category.show(truncate=False)

spark.stop()
