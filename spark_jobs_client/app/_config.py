FIELD_MAPPING_BY_COUNTRY = {
    "feilv": {
            "Order ID": "Order_ID",
            "Order Status": "Order_Status",
            "Order Creation Date": "Purchase_Datetime", # 映射到 Purchase_Datetime (订购时间)
            "Order Paid Time": "Payment_Datetime",     # 映射到 Payment_Datetime (付款时间)
            "Tracking Number*": "Tracking_Number",     # 映射到 Tracking_Number (运单号)
            "Shipping Option": "Logistics_Method",     # 映射到 Logistics_Method (物流方式)
            "Ship Time": "Ship_Datetime",              # 映射到 Ship_Datetime (发货时间)
            "Estimated Ship Out Date": "Estimated_Ship_Out_Datetime", # 映射到 Estimated_Ship_Out_Datetime (标发时间)
            "Country": "Ship_Country",                 # 映射到 Ship_Country (国家/地区)
            "Estimated Order Weight": "Estimated_Weight_g", # Excel的Order Weight通常指g，映射到Estimated_Weight_g
            "Zip Code": "Zip_Code",
            "Product Name": "Product_Name",
            "Variation Name": "Variation_Name",
            "Shipment Method": "Shipment_Method",      # 映射到 Shipment_Method (订单类型/发货方式)
            "Cancel reason": "Cancel_Reason",
            "Return / Refund Status": "Return_Refund_Status",
            "Original Price": "Original_Price",
            "SKU Reference No.": "Product_ID",                # 映射到 Product_ID
            "Deal Price": "Deal_Price",
            "Quantity": "Quantity",
            "Returned quantity": "Return_Quantity",
            "Price Discount(from Seller)(PHP)": "Item_Promotion_Discount", # 映射到 Item_Promotion_Discount (商品折扣)
            "Credit Card Discount Total(PHP)": "Credit_Card_Discount_Total", # 映射到 Credit_Card_Discount_Total (信用卡折扣)
            "Remark from buyer": "Remark_From_Buyer",
            "Bundle Deals Indicator(Y/N)": "Bundle_Deals_Indicator", 
            "Note": "Note",                             # 通常是客服备注
            "Receiver Name": "Recipient",
            "Phone Number": "Phone",
            "Delivery Address": "Address_Line_1",       # 默认映射到 Address_Line_1，可能需要组合多行
            "Province": "Ship_State",
            "City": "Ship_City",
            "District": "District",
            "Town": "Town",
            "Username (Buyer)": "Username_Buyer",
            "Order Complete Time": "Order_Complete_Datetime",
            "Number of Items in Order": "Number_Of_Pieces",                 # 假设指的是商品总件数
            "Order Total Weight": "Order_Total_Weight_g",                     # 这个可能需要从所有SKU重量汇总，此处先映射到预估总重
            "Seller Voucher(PHP)": "Store_Voucher",                         # 映射到 Store_Voucher (店铺优惠券)
            "Shopee Voucher(PHP)": "Platform_Voucher",                      # 映射到 Platform_Voucher (平台优惠券)
            "Shopee Bundle Discount(PHP)": "Platform_Bundle_Discount",      # 映射到 Platform_Bundle_Discount (平台组合折扣)
            "Seller Bundle Discount(PHP)": "Seller_Bundle_Discount",        # 映射到 Seller_Bundle_Discount (卖家组合折扣)
            "Shopee Coins Offset(PHP)": "Coin_Offset",                      # 映射到 Coin_Offset (金币抵扣)
            "Buyer Paid Shipping Fee": "Buyer_Paid_Shipping_Fee",           # 映射到 Buyer_Paid_Shipping_Fee (买家支付运费)
            "Products' Price Paid by Buyer (PHP)": "Products_Price_Paid_By_Buyer", # 映射到 Products_Price_Paid_By_Buyer (实付金额)
            "Shipping Rebate Estimate": "Shipping_Cost",                    # 运费回扣估算，可能映射到 Shipping_Cost (物流运费) 或其他
            "Estimated Shipping Fee": "Estimated_Shipping_Cost_CNY",        # 估算运费，如果PHP是统一的，这里是CNY可能需要注意
            "Reverse Shipping Fee": "Reverse_Shipping_Fee",
            "Service Fee": "Service_Fee",
            "SKU Total Weight": "SKU_Total_Weight",                 # SKU的总重量，如果是一个订单多个SKU，可能需要聚合
            "Grand Total": "Total_Amount",
            "Parent SKU Reference No.": "Tags",                     # 根据你提供的注释映射到 Tags
    },
    "taiguo": {
        "หมายเลขคำสั่งซื้อ": "Order_ID",
        "สถานะการสั่งซื้อ": "Order_Status",
        "วันที่ทำการสั่งซื้อ": "Purchase_Datetime",                 # 映射到 Purchase_Datetime (订购时间)
        "เวลาการชำระสินค้า": "Payment_Datetime",           # 映射到 Payment_Datetime (付款时间)
        "*หมายเลขติดตามพัสดุ": "Tracking_Number",  
        "ตัวเลือกการจัดส่ง": "Logistics_Method",                # 映射到 Logistics_Method (物流方式)
        "เวลาส่งสินค้า": "Ship_Datetime",                  # 映射到 Ship_Datetime (发货时间)
        "วันที่คาดว่าจะทำการจัดส่งสินค้า": "Estimated_Ship_Out_Datetime", # 映射到 Estimated_Ship_Out_Datetime (标发时间)
        "ประเทศ": "Ship_Country",    
        "รหัสไปรษณีย์": "Zip_Code",
        "ชื่อสินค้า": "Product_Name",
        "ชื่อตัวเลือก": "Variation_Name",
        "วิธีการจัดส่ง": "Shipment_Method",                     # 映射到 Shipment_Method (订单类型/发货方式)
        "เหตุผลในการยกเลิกคำสั่งซื้อ": "Cancel_Reason",
        "สถานะการคืนเงินหรือคืนสินค้า": "Return_Refund_Status",
        "ราคาตั้งต้น": "Original_Price",
        "เลขอ้างอิง SKU (SKU Reference No.)": "Product_ID",                # 映射到 Product_ID
        "ราคาสินค้าที่ชำระโดยผู้ซื้อ (THB)": "Deal_Price",
        "จำนวน": "Quantity",
        "จำนวนที่ส่งคืน": "Return_Quantity",
        "หมายเหตุจากผู้ซื้อ": "Remark_From_Buyer",
        "ชื่อผู้รับ": "Recipient",
        "หมายเลขโทรศัพท์": "Phone",
        "ที่อยู่ในการจัดส่ง": "Address_Line_1",
        "จังหวัด": "Ship_State",
        "เขต/อำเภอ": "District",
        "ชื่อผู้ใช้ (ผู้ซื้อ)": "Username_Buyer",
        "เวลาที่ทำการสั่งซื้อสำเร็จ": "Order_Complete_Datetime",
        "น้ำหนักสินค้าตามจริงต่อคำสั่งซื้อ": "Order_Total_Weight_g",                     # 这个可能需要从所有SKU重量汇总，此处先映射到预估总重
        "ส่วนลดจากการใช้เหรียญ": "Coin_Offset",                      # 映射到 Coin_Offset (金币抵扣)
        "ค่าจัดส่งที่ชำระโดยผู้ซื้อ": "Buyer_Paid_Shipping_Fee",           # 映射到 Buyer_Paid_Shipping_Fee (买家支付运费)
        "ราคาสินค้าที่ชำระโดยผู้ซื้อ (THB)": "Products_Price_Paid_By_Buyer", # 映射到 Products_Price_Paid_By_Buyer (实付金额)
        "ค่าจัดส่งโดยประมาณ": "Estimated_Shipping_Cost_CNY",        # 估算运费，如果PHP是统一的，这里是CNY可能需要注意
        "ค่าจัดส่งสินค้าคืน": "Reverse_Shipping_Fee",
        "ค่าบริการ": "Service_Fee",
        "วันที่คำสั่งซื้อถูกยกเลิก": "Order_Cancel_Datetime",
        "ค่าธรรมเนียม (%)":"Handling_Fee",
        "ช่องทางการชำระเงิน":"Payment_Method",
        "Transaction Fee": "Transaction_Fee",
        "ค่าคอมมิชชั่น": "Commission_Fee",
        "จำนวนเงินทั้งหมด": "Total_Amount",
        "เลขอ้างอิง Parent SKU": "Tags", 
    },
    "yuenan": {
        "Mã đơn hàng": "Order_ID",
        "Trạng Thái Đơn Hàng": "Order_Status", 
        "Ngày đặt hàng": "Purchase_Datetime",               # 映射到 Purchase_Datetime (订购时间)       
        "Thời gian đơn hàng được thanh toán": "Payment_Datetime",     # 映射到 Payment_Datetime (付款时间)      
        "Mã vận đơn": "Tracking_Number",        
        "Đơn Vị Vận Chuyển": "Logistics_Method",        # 映射到 Logistics_Method (物流方式)       
        "Ngày xuất hàng": "Ship_Datetime",              # 映射到 Ship_Datetime (发货时间)       
        "Quốc gia": "Ship_Country",            
        "Tên sản phẩm": "Product_Name",     
        "Tên phân loại hàng": "Variation_Name",  
        "Loại đơn hàng": "Shipment_Method",                     # 映射到 Shipment_Method (订单类型/发货方式)       
        "Lý do hủy": "Cancel_Reason",       
        "Trạng thái Trả hàng/Hoàn tiền": "Return_Refund_Status",   
        "Giá gốc": "Original_Price",     
        "SKU phân loại hàng": "Product_ID",                # 将产品变体SKU映射到 Product_ID     

        "Số lượng": "Quantity",      
        "Số lượng sản phẩm được hoàn trả": "Return_Quantity",     
        "Số tiền được giảm khi thanh toán bằng thẻ Ghi nợ": "Credit_Card_Discount_Total", # 映射到 Credit_Card_Discount_Total (信用卡折扣)
        "Nhận xét từ Người mua": "Remark_From_Buyer",
        "Ghi chú": "Note",                          # 通常是客服备注
        "Tên Người nhận": "Recipient",
        "Số điện thoại": "Phone",
        "Địa chỉ nhận hàng": "Address_Line_1",
        "Tỉnh/Thành phố": "Ship_State",
        "TP / Quận / Huyện": "Ship_City",
        "Quận": "District",
        "Người Mua": "Username_Buyer",
        "Thời gian hoàn thành đơn hàng": "Order_Complete_Datetime",
        "Tổng cân nặng": "Order_Total_Weight_g",                     # 这个可能需要从所有SKU重量汇总，此处先映射到预估总重
        "Mã giảm giá của Shopee": "Store_Voucher",                         # 映射到 Store_Voucher (店铺优惠券)
        "Giảm giá từ combo Shopee": "Platform_Bundle_Discount",      # 映射到 Platform_Bundle_Discount (平台组合折扣)
        "Giảm giá từ Combo của Shop": "Seller_Bundle_Discount",        # 映射到 Seller_Bundle_Discount (卖家组合折扣)
        "Phí vận chuyển mà người mua trả": "Buyer_Paid_Shipping_Fee",           # 映射到 Buyer_Paid_Shipping_Fee (买家支付运费)
        "Tổng giá bán (sản phẩm)": "Products_Price_Paid_By_Buyer", # 映射到 Products_Price_Paid_By_Buyer (实付金额)
        "Phí vận chuyển (dự kiến)": "Estimated_Shipping_Cost_CNY",        # 估算运费，如果PHP是统一的，这里是CNY可能需要注意
        "Phí trả hàng": "Reverse_Shipping_Fee",
        "Phí Dịch Vụ": "Service_Fee",
        "Ngày hủy thành công": "Order_Cancel_Datetime",
        "Phí thanh toán":"Handling_Fee",
        "Phương thức thanh toán":"Payment_Method",
        "Ngày giao hàng dự kiến": "Estimated_Ship_Out_Datetime",
        "Cân nặng sản phẩm": "SKU_Total_Weight",                 # SKU的总重量，如果是一个订单多个SKU，可能需要聚合
        "Tổng số tiền người mua thanh toán": "Total_Amount",
        "Mã Kiện Hàng": "Package_ID",
        "Sản Phẩm Bán Chạy": "Is_Best_Selling_Product"
    },
    "malai": {
        "Order ID": "Order_ID",
        "Order Status": "Order_Status",
        "Order Creation Date": "Purchase_Datetime", # 映射到 Purchase_Datetime (订购时间)
        "Order Paid Time": "Payment_Datetime",     # 映射到 Payment_Datetime (付款时间)
        "Tracking Number*": "Tracking_Number",     # 映射到 Tracking_Number (运单号)
        "Shipping Option": "Logistics_Method",     # 映射到 Logistics_Method (物流方式)
        "Ship Time": "Ship_Datetime",              # 映射到 Ship_Datetime (发货时间)
        "Estimated Ship Out Date": "Estimated_Ship_Out_Datetime", # 映射到 Estimated_Ship_Out_Datetime (标发时间)
        "Country": "Ship_Country",                 # 映射到 Ship_Country (国家/地区)
        "Estimated Order Weight": "Estimated_Weight_g", # Excel的Order Weight通常指g，映射到Estimated_Weight_g
        "Zip Code": "Zip_Code",
        "Product Name": "Product_Name",
        "Variation Name": "Variation_Name",
        "Shipment Method": "Shipment_Method",      # 映射到 Shipment_Method (订单类型/发货方式)
        "Cancel reason": "Cancel_Reason",
        "Return / Refund Status": "Return_Refund_Status",
        "Original Price": "Original_Price",
        "SKU Reference No.": "Product_ID",   
        "Deal Price": "Deal_Price",
        "Quantity": "Quantity",
        "Returned quantity": "Return_Quantity",
        "Seller Discount": "Item_Promotion_Discount", # 映射到 Item_Promotion_Discount (商品折扣)
        "Credit Card Discount Total": "Credit_Card_Discount_Total", # 映射到 Credit_Card_Discount_Total (信用卡折扣)
        "Remark from buyer": "Remark_From_Buyer",
        "Note": "Note",                             # 通常是客服备注
        "Receiver Name": "Recipient",
        "Phone Number": "Phone",
        "Delivery Address": "Address_Line_1",       # 默认映射到 Address_Line_1，可能需要组合多行
        "Province": "Ship_State",
        "City": "Ship_City",
        "District": "District",
        "Town": "Town",
        "Username (Buyer)": "Username_Buyer",
        "Order Complete Time": "Order_Complete_Datetime",
        "Order Total Weight": "Order_Total_Weight_g",                     # 这个可能需要从所有SKU重量汇总，此处先映射到预估总重
        "Seller Voucher": "Store_Voucher",                         # 映射到 Store_Voucher (店铺优惠券)
        "Shopee Voucher": "Platform_Voucher",                      # 映射到 Platform_Voucher (平台优惠券)
        "Shopee Bundle Discount": "Platform_Bundle_Discount",      # 映射到 Platform_Bundle_Discount (平台组合折扣)
        "Seller Bundle Discount": "Seller_Bundle_Discount",        # 映射到 Seller_Bundle_Discount (卖家组合折扣)
        "Shopee Coins Offset": "Coin_Offset",                      # 映射到 Coin_Offset (金币抵扣)
        "Buyer Paid Shipping Fee": "Buyer_Paid_Shipping_Fee",           # 映射到 Buyer_Paid_Shipping_Fee (买家支付运费)
        "Product Subtotal": "Products_Price_Paid_By_Buyer",             # 映射到 Products_Price_Paid_By_Buyer (实付金额)
        "Shipping Rebate Estimate": "Shipping_Cost",                    # 运费回扣估算，可能映射到 Shipping_Cost (物流运费) 或其他
        "Estimated Shipping Fee": "Estimated_Shipping_Cost_CNY",        # 估算运费，如果PHP是统一的，这里是CNY可能需要注意
        "Reverse Shipping Fee": "Reverse_Shipping_Fee",
        "Service Fee": "Service_Fee",
        "SKU Total Weight": "SKU_Total_Weight",                 # SKU的总重量，如果是一个订单多个SKU，可能需要聚合
        "Transaction Fee": "Transaction_Fee",
        "Commission Fee": "Commission_Fee",
        "Grand Total": "Total_Amount",
        "Parent SKU Reference No.": "Tags"    
    },
    "yinni": {
        "No. Pesanan": "Order_ID",
        "Status Pesanan": "Order_Status",
        "Waktu Pesanan Dibuat": "Purchase_Datetime", # 映射到 Purchase_Datetime (订购时间)
        "Waktu Pembayaran Dilakukan": "Payment_Datetime",     # 映射到 Payment_Datetime (付款时间)
        "No. Resi": "Tracking_Number",              # 映射到 Tracking_Number (运单号)
        "Opsi Pengiriman": "Logistics_Method",     # 映射到 Logistics_Method (物流方式)
        "Waktu Pengiriman Diatur": "Ship_Datetime",              # 映射到 Ship_Datetime (发货时间)
        "Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)": "Estimated_Ship_Out_Datetime", # 映射到 Estimated_Ship_Out_Datetime (标发时间)
        "Country": "Ship_Country",                 # 映射到 Ship_Country (国家/地区)
        "Nama Produk": "Product_Name",
        "Nama Variasi": "Variation_Name",
        "Alasan Pembatalan": "Cancel_Reason",
        "Status Pembatalan/ Pengembalian": "Return_Refund_Status",
        "Harga Awal": "Original_Price",
        "Nomor Referensi SKU": "Product_ID",   
        "Jumlah": "Quantity",
        "Returned quantity": "Return_Quantity",
        "Diskon Dari Penjual": "Item_Promotion_Discount", # 映射到 Item_Promotion_Discount (商品折扣)
        "Diskon Kartu Kredit": "Credit_Card_Discount_Total", # 映射到 Credit_Card_Discount_Total (信用卡折扣)
        "Catatan dari Pembeli": "Remark_From_Buyer",
        "Catatan": "Note",                           # 通常是客服备注
        "Nama Penerima": "Recipient",
        "No. Telepon": "Phone",
        "Alamat Pengiriman": "Address_Line_1",       # 默认映射到 Address_Line_1，可能需要组合多行
        "Provinsi": "Ship_State",
        "Kota/Kabupaten": "Ship_City",
        "Username (Pembeli)": "Username_Buyer",
        "Waktu Pesanan Selesai": "Order_Complete_Datetime",
        "Total Berat": "Order_Total_Weight_g",                     # 这个可能需要从所有SKU重量汇总，此处先映射到预估总重
        "Voucher Ditanggung Penjual": "Store_Voucher",                         # 映射到 Store_Voucher (店铺优惠券)
        "Voucher Ditanggung Shopee": "Platform_Voucher",                      # 映射到 Platform_Voucher (平台优惠券)
        "Paket Diskon (Diskon dari Shopee)": "Platform_Bundle_Discount",      # 映射到 Platform_Bundle_Discount (平台组合折扣)
        "Paket Diskon (Diskon dari Penjual)": "Seller_Bundle_Discount",        # 映射到 Seller_Bundle_Discount (卖家组合折扣)
        "Potongan Koin Shopee": "Coin_Offset",                      # 映射到 Coin_Offset (金币抵扣)
        "Ongkos Kirim Dibayar oleh Pembeli": "Buyer_Paid_Shipping_Fee",           # 映射到 Buyer_Paid_Shipping_Fee (买家支付运费)
        "Total Harga Produk": "Products_Price_Paid_By_Buyer",       # 映射到 Products_Price_Paid_By_Buyer (实付金额)
        "Estimasi Potongan Biaya Pengiriman": "Shipping_Cost",                    # 运费回扣估算，可能映射到 Shipping_Cost (物流运费) 或其他
        "Perkiraan Ongkos Kirim": "Estimated_Shipping_Cost_CNY",        # 估算运费，如果PHP是统一的，这里是CNY可能需要注意
        "Ongkos Kirim Pengembalian Barang": "Reverse_Shipping_Fee",
        "Metode Pembayaran":"Payment_Method",
        "Berat Produk": "SKU_Total_Weight",                 # SKU的总重量，如果是一个订单多个SKU，可能需要聚合
        "Total Pembayaran": "Total_Amount"
    },
    "tiktok": {
        "Order ID": "Order_ID",
        "Order Status": "Order_Status",
        "Created Time": "Purchase_Datetime",        # 映射到 Purchase_Datetime (订购时间)
        "Paid Time": "Payment_Datetime",            # 映射到 Payment_Datetime (付款时间)
        "Tracking ID": "Tracking_Number",           # 映射到 Tracking_Number (运单号)
        "Delivery Option": "Logistics_Method",      # 映射到 Logistics_Method (物流方式)
        "Shipped Time": "Ship_Datetime",              # 映射到 Ship_Datetime (发货时间)
        "Country": "Ship_Country",     
        "Zipcode": "Zip_Code",
        "Product Name": "Product_Name",
        "Variation": "Variation_Name",
        "Cancelation/Return Type": "Cancel_Reason",
        "SKU Unit Original Price": "Original_Price",
        "Seller SKU": "Product_ID",   
        "Quantity": "Quantity",
        "Sku Quantity of return": "Return_Quantity",
        "SKU Seller Discount": "Item_Promotion_Discount", # 映射到 Item_Promotion_Discount (商品折扣)
        "Seller Note": "Note",                      # 通常是客服备注
        "Recipient": "Recipient",
        "Phone #": "Phone",
        "Detail Address": "Address_Line_1",         # 默认映射到 Address_Line_1，可能需要组合多行
        "Province": "Ship_State",
        "District": "Ship_City",
        "Buyer Username": "Username_Buyer",
        "Weight(kg)":"Weight_kg",
        "Shipping Fee Platform Discount": "Shipping_Cost",                    # 运费回扣估算，可能映射到 Shipping_Cost (物流运费) 或其他
        "Cancelled Time": "Order_Cancel_Datetime",
        "Payment Method":"Payment_Method",
    },
    "lazada": {
        "orderItemId": "Order_ID",
        "status": "Order_Status",
        "createTime": "Purchase_Datetime",                          # 映射到 Purchase_Datetime (订购时间)
        "trackingCode": "Tracking_Number",                          # 映射到 Tracking_Number (运单号)
        "deliveryType": "Logistics_Method",                         # 映射到 Logistics_Method (物流方式)
        "promisedShippingTime": "Estimated_Ship_Out_Datetime",      # 映射到 Estimated_Ship_Out_Datetime (标发时间)
        "billingCountry": "Ship_Country",  

        "billingPostCode": "Zip_Code",
        "itemName": "Product_Name",
        "variation": "Variation_Name",
        "orderType": "Shipment_Method",                             # 映射到 Shipment_Method (订单类型/发货方式)
        "sellerSku": "Product_ID",   

        "sellerDiscountTotal": "Item_Promotion_Discount",           # 映射到 Item_Promotion_Discount (商品折扣)
        "sellerNote": "Note",                                       # 通常是客服备注
        "shippingName": "Recipient",
        "billingPhone": "Phone",
        "shippingAddress": "Address_Line_1",                        # 默认映射到 Address_Line_1，可能需要组合多行
        "shippingCity": "Ship_State",
        "shippingRegion": "Ship_City",
        "payMethod":"Payment_Method",
        "paidPrice": "Total_Amount",
    }
}

# 你的标准化可查询列
ALLOWED_DB_COLUMNS = [

]

# --- MinIO 桶和路径定义 ---
RAW_DATA_BUCKET = "raw-data"
UNCLASSIFIED_ROOT_PREFIX = "unclassified/" # 所有原始文件落地的根目录
CLASSIFIED_PREFIX = "classified/"
UNIDENTIFIED_RAW_PREFIX = "unidentified_raw/" # 存放无法识别的文件
# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',                    # 数据库主机地址
    'user': 'root',                      # 数据库用户名
    'password': 'Jiaoji123!',                # 数据库密码
    'database': 'test',                        # 要连接的数据库名
    'port': 3306,                               # MySQL 默认端口
    'charset':'utf8mb4'
}

ALLOWED_DB_COLUMNS = [
    "ID",
    "Ship_Country",
    "Order_ID",
    "Store",
    "Purchase_Datetime",

    "Currency",
    "Region",
    "Order_Status",
    "Payment_Datetime",
    "Tracking_Number",
    "Logistics_Method",
    "Ship_Datetime",
    "Estimated_Ship_Out_Datetime",
    "Estimated_Weight_g",
    "Zip_Code",
    "Product_Name",
    "Variation_Name",
    "Shipment_Method",
    "Cancel_Reason",
    "Return_Refund_Status",
    "Original_Price",
    "Product_ID",
    "Deal_Price",
    "Quantity",
    "Return_Quantity",
    "Item_Promotion_Discount",
    "Credit_Card_Discount_Total",
    "Remark_From_Buyer",
    "Bundle_Deals_Indicator",
    "Note",
    "Recipient",
    "Phone",
    "Address_Line_1",
    "Ship_State",
    "Ship_City",
    "District",
    "Town",
    "Username_Buyer",
    "Order_Complete_Datetime",
    "Number_Of_Pieces",
    "Order_Total_Weight_g",
    "Store_Voucher",
    "Platform_Voucher",
    "Platform_Bundle_Discount",
    "Seller_Bundle_Discount",
    "Coin_Offset",
    "Buyer_Paid_Shipping_Fee",
    "Products_Price_Paid_By_Buyer",
    "Shipping_Cost",
    "Estimated_Shipping_Cost_CNY",
    "Reverse_Shipping_Fee",
    "Service_Fee",
    "SKU_Total_Weight",
    "Total_Amount",
    "Tags",
    "Order_Cancel_Datetime",
    "Handling_Fee",
    "Payment_Method",
    "Transaction_Fee",
    "Commission_Fee",
    "Package_ID",
    "Is_Best_Selling_Product",
    "Weight_kg"
]


DATETIME_COLUMNS_CLEANERS = {
    "Purchase_Datetime",
    "Payment_Datetime",
    "Ship_Datetime",
    "Estimated_Ship_Out_Datetime",
    "Order_Complete_Datetime",
    "Order_Cancel_Datetime",
}

COUNTRY_INFO = {
    'PH': {"Currency": "PHP", "Region":"东南亚", "Timezone":'Asia/Manila'      },# 菲律宾 (Philippines) - 马尼拉时间 (UTC+8)
    'TH': {"Currency": "THB", "Region":"东南亚", "Timezone":'Asia/Bangkok'     },# 泰国 (Thailand) - 曼谷时间 (UTC+7)
    'VN': {"Currency": "VND", "Region":"东南亚", "Timezone":'Asia/Ho_Chi_Minh' },# 越南 (Vietnam) - 胡志明时间 (UTC+7)
    'SG': {"Currency": "SGD", "Region":"东南亚", "Timezone":'Asia/Singapore'   },# 新加坡 (Singapore) - 新加坡时间 (UTC+8)
    'ID': {"Currency": "IDR", "Region":"东南亚", "Timezone":'Asia/Jakarta'     },# 印度尼西亚 (Indonesia) - 雅加达时间 (UTC+7, 西部)
    'MY': {"Currency": "MYR", "Region":"东南亚", "Timezone":'Asia/Kuala_Lumpur'},# 马来西亚 (Malaysia) - 吉隆坡时间 (UTC+8)

}