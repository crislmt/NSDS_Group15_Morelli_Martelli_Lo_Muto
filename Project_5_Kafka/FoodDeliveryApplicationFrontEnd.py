import requests
from requests.exceptions import HTTPError
import json

CUSTOMER_SERVICE_URL='http://192.168.56.102:9090'
ORDER_SERVICE_URL="http://192.168.56.103:9090/order"
SHIPPING_SERVICE_URL="http://192.168.56.104:9090/shippings"

REGULAR_USER="REGULAR_USER"
ADMIN="ADMIN"
DELIVERY_MAN="DELIVERY_MAN"

def registration():
    name=input("insert name: ")
    surname=input("insert surname: ")
    email=input("insert email: ")
    address=input("insert address: ")
    param={'name':name, 'surname':surname,'email':email, 'address':address}
    response=requests.post(CUSTOMER_SERVICE_URL
+ "/register", param)
    if response.status_code==200:
        print("You have successfully registered")
    else:
        print(response.content)
def add_product():
    product_name=input("insert product name: ")
    quantity=input("insert quantity: ")
    param={'productName':product_name, 'quantity':quantity}
    response=requests.put(ORDER_SERVICE_URL+'/addProduct', params=param)
    if response.status_code==200:
        print("The product has been successfully added")
    else:
        print(response.content)

def update_product():
    product_name=input("insert product name: ")
    quantity=input("insert quantity: ")
    param={'productName':product_name, 'quantity':quantity}
    response=requests.post(ORDER_SERVICE_URL+ '/updateProduct',params=param)
    if response.status_code==200:
        print("The product has been successfully updated")
    else:
        print(response.content)

def insert_order():
    email=input("insert email: ")
    product_name=input("insert product name: ")
    quantity=input("insert quantity: ")
    shipping_address=input("insert shipping address: ")
    data={"customerEmail": email, "productName": product_name, "quantity":quantity,"shippingAddress":shipping_address }
    jsonData=json.dumps(data)
    headers={'Content-Type':'application/json'}
    response=requests.post(ORDER_SERVICE_URL+ '/insertOrder', data=jsonData, headers=headers)
    if response.status_code==200:
        print("The order has been successfully inserted")
    else:
        print(response.content)
def deliver_shipping():
    code=int(input("insert code: "))
    response=requests.put(SHIPPING_SERVICE_URL+ '/deliverOrder', params={'code':code})
    if response.status_code==200:
        print("OK")
    else:
        print(response.content)

def get_shippings_by_email():
    email=input("insert email: ")
    response=requests.get(SHIPPING_SERVICE_URL+'/getShippingsByEmail', params={'email':email})
    print(response.content)
def get_shippings():
    response=requests.get(SHIPPING_SERVICE_URL+'/getShippings')
    print(response.content)



while(True):
    print("Please select user type\n");
    print("1. Regular user")
    print("2. Admin")
    print("3. Delivery man")

    user_type=REGULAR_USER
    print("\n")

    command=input()
    match command:
        case "1":
            user_type=REGULAR_USER
            print("\n")
        case "2":
            user_type=ADMIN
            print("\n")
        case "3":
            user_type=DELIVERY_MAN
            print("\n")
        case other:
            print("Bad input\n")
            continue
        
    while(True):
        if user_type==REGULAR_USER :
            print("Hello user, what do you want to do?\n")
            print("1. Register")
            print("2. Insert an order")
            print("3. Get all orders by email")
            print("4. Switch user type")
            print("\n")
            command=input()
            match command:
                case "1":
                    registration()
                    print("\n")
                case "2":
                    insert_order()
                    print("\n")
                case  "3":
                    get_shippings_by_email()
                    print("\n")
                case "4":
                    print("\n")
                    break
                case other:
                    print("Bad input\n")
                    continue

        elif user_type==ADMIN:
            print("Hello admin, what do you want to do?\n")
            print("1. Register")
            print("2. Add a product")
            print("3. Update a product")
            print("4. Insert an order")
            print("5. Deliver an Order")
            print("6. Get all orders by email")
            print("7. Get all orders")
            print("8: Switch user type")
            print("\n")
            command=input()
            match command:
                case "1":
                    registration()
                    print("\n")
                case "2":
                    add_product()
                    print("\n")
                case "3":
                    update_product()
                    print("\n")
                case "4":
                    insert_order()
                    print("\n")
                case "5":
                    deliver_shipping()
                    print("\n")
                case  "6":
                    get_shippings_by_email()
                    print("\n")
                case "7":
                    get_shippings()
                    print("\n")
                case "8":
                    print("\n")

                    break
                case other:
                    print("Bad input\n")
                    continue
        else:
            print("Hello delivery man, What do you want to do?")
            print("1. Deliver an Order")
            print("2. Get all orders")
            print("3. Switch user type")
            print("\n")
            command=input()
            match command:
                case "1":
                    deliver_shipping()
                    print("\n")
                case "2":
                    get_shippings()
                    print("\n")
                case "8":
                    print("\n")
                    break
                case other:
                    print("Bad input\n")
                    continue


    


           
        