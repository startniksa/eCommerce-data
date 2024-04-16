-- Creation of tables
CREATE TABLE IF NOT EXISTS vendors (
    vendor_name TEXT PRIMARY KEY,
    shipping_cost INTEGER,
    customer_review_score DOUBLE PRECISION,
    number_of_feedbacks INTEGER,
    updated_on TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    item TEXT,
    category TEXT,
    vendor TEXT,
    sale_price DOUBLE PRECISION,
    stock_status TEXT,
    updated_on TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (item, vendor),
    FOREIGN KEY (vendor) REFERENCES vendors(vendor_name)
        ON UPDATE CASCADE ON DELETE RESTRICT
);
