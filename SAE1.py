# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn = sqlite3.connect("ClassicModel.sqlite")

# Récupération du contenu des tables avec une requête SQL
customers = pandas.read_sql_query("SELECT * FROM Customers;", conn)

orders = pandas.read_sql_query("SELECT * FROM Orders;", conn)

orderDetails = pandas.read_sql_query("SELECT * FROM OrderDetails;", conn)

employees = pandas.read_sql_query("SELECT * FROM Employees;", conn)

payments = pandas.read_sql_query("SELECT * FROM Payments;", conn)





# 1. Lister les clients n’ayant jamais effecuté une commande ;
q1 = pandas.read_sql_query(
                            """
                            SELECT c.customerNumber, c.customerName
                            FROM Customers c
                            LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
                            WHERE o.orderNumber IS NULL
                            """,
                            conn
)
print(q1)

# 2. Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci ;
q2 = pandas.read_sql_query(
                            """
                            SELECT
                                e.employeeNumber,
                                e.firstName,
                                e.lastName,
                                COUNT(DISTINCT c.customerNumber) AS numberOfCustomers,
                                COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
                                SUM(p.amount) AS totalOrderAmount
                            FROM Employees e
                            LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber
                            LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
                            LEFT JOIN Payments p ON o.customerNumber = p.customerNumber
                            GROUP BY e.employeeNumber, e.firstName, e.lastName;
                            """,
                            conn
)
print(q2)

# 3. Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a ;
q3 = pandas.read_sql_query(
                            """
                            SELECT
                                of.officeCode,
                                of.city,
                                of.country AS officeCountry,
                                COUNT(DISTINCT c.customerNumber) AS numberOfCustomers,
                                COUNT(DISTINCT od.orderNumber) AS numberOfOrders,
                                SUM(p.amount) AS totalOrderAmount,
                                COUNT(DISTINCT CASE
                                      WHEN c.country != of.country THEN c.customerNumber
                                      ELSE NULL
                                END) AS customersFromDifferentCountry
                            FROM Offices of
                            LEFT JOIN Employees e ON of.officeCode = e.officeCode
                            LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber
                            LEFT JOIN Orders od ON c.customerNumber = od.customerNumber
                            LEFT JOIN Payments p ON od.customerNumber = p.customerNumber
                            GROUP BY of.officeCode, of.city, of.country;
                            """,
                            conn
)
print(q3)

# 4. Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents ;
# 4. Pour chaque produit, donner le nombre de commandes,
#la quantité totale commandée, et le nombre de clients différents ;
q4 = pandas.read_sql_query(
                            """
                            SELECT
                            o.productcode,
                            p.productname,
                            COUNT(ord.ordernumber) as numberOfOrders,
                            SUM(DISTINCT o.quantityordered) as quantityOrdered,
                            COUNT(DISTINCT c.customernumber) as numberOfCustomers
                            FROM Products p
                            LEFT JOIN OrderDetails o on p.productcode = o.productCode
                            LEFT JOIN Orders ord on o.ordernumber = ord.orderNumber
                            LEFT JOIN Customers c on ord.customerNumber = c.customerNumber
                            GROUP BY
                            o.productCode,
                            p.productName  
                            """,
                            conn
)
print(q4)

# 5. Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes
#et le montant total payé : on veut conserver les clients n’ayant jamais commandé
#dans le résultat final ;
q5 = pandas.read_sql_query(
                            """
                            SELECT
                                c.country,
                                COUNT(DISTINCT od.orderNumber) AS numberOfOrders,
                                odt.quantityOrdered*priceEach AS totalOrderedAmount,
                                p.amount AS totalPaidAmount
                            FROM Customers c
                            LEFT JOIN Orders od ON c.customerNumber = od.customerNumber
                            LEFT JOIN Payments p ON od.customerNumber = p.customerNumber
                            LEFT JOIN OrderDetails odt ON odt.orderNumber = od.orderNumber
                            GROUP BY c.country;
                            """,
                            conn
)
print(q5)

# 6. On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client ;
q6 = pandas.read_sql_query(
                            """
                            SELECT
                                p.productline,
                                c.country,
                                COUNT(DISTINCT ord.ordernumber) as numberOfOrders
                            FROM Products p
                            LEFT JOIN OrderDetails o on p.productCode = o.productCode
                            LEFT JOIN Orders ord on o.orderNumber = ord.orderNumber
                            LEFT JOIN Customers c on ord.customerNumber = c.customerNumber
                            GROUP BY p.productLine, c.country;
                            """,
                            conn
)
print(q6)

# 7. On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule ;
q7 = pandas.read_sql_query(
                            """
                            select
                                p.productCode,
                                p.productLine,
                                c.country,
                                SUM(py.amount) as payment
                            FROM products p
                            LEFT JOIN OrderDetails o on p.productCode = o.productCode
                            LEFT JOIN Orders ord on o.orderNumber = ord.orderNumber
                            LEFT JOIN Customers c on ord.customerNumber = c.customerNumber
                            LEFT JOIN Payments py on c.customerNumber = py.customerNumber
                            group by
                            p.productLine,
                            c.country
                            """,
                            conn
)
print(q7)

# 8. Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach) ;
q8 = pandas.read_sql_query(
                            """
                            SELECT
                                p.productCode,
                                p.productName,
                                p.buyPrice - priceEach AS margin
                            FROM Products p
                            LEFT JOIN OrderDetails o on p.productCode = o.productCode
                            ORDER BY margin DESC LIMIT 10;
                            """,
                            conn
)
print(q8)

# 9. Lister les produits (avec le nom et le code du client) qui ont été vendus à perte :
#           - Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois,
#           - Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;
q9 = pandas.read_sql_query(
                            """
                            SELECT
                                p.productCode,
                                p.productName,
                                c.customerNumber,
                                c.contactLastName,
                                p.buyPrice - priceEach AS margin
                            FROM Products p
                            LEFT JOIN OrderDetails o on p.productCode = o.productCode
                            LEFT JOIN Orders ord on o.orderNumber = ord.orderNumber
                            LEFT JOIN Customers c on ord.customerNumber = c.customerNumber
                            WHERE margin < 0
                            ORDER BY margin;
                            """,
                            conn
)
print(q9)


#10. Lister les clients pour lesquels le montant total payé est inférieur aux montants totals des achats ;
q10 = pandas.read_sql_query(
                            """
                            select
                            ord.Customernumber,
                            cu.customername,
                            sum(distinct o.priceeach) as priceEach,
                            sum(distinct p.amount) as amount
                            from customers cu
                            left join orders ord on cu.customernumber = ord.customernumber
                            left join orderdetails o on o.ordernumber = ord.ordernumber
                            left join products pd on o.productcode = pd.productcode
                            left join payments p on cu.customernumber = p.customernumber
                            WHERE priceEach > amount
                            group by
                            ord.Customernumber
                           
                            """,
                            conn
)
print(q10)

# Fermeture de la connexion
conn.close()