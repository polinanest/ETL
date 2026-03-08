db = db.getSiblingDB('user_analytics');

db.createCollection("user_sessions", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["session_id", "user_id", "start_time", "pages_visited", "device"],
            properties: {
                session_id: { bsonType: "string" },
                user_id: { bsonType: "string" },
                start_time: { bsonType: "date" },
                end_time: { bsonType: "date" },
                pages_visited: { bsonType: "array" },
                device: { bsonType: "object" },
                actions: { bsonType: "array" }
            }
        }
    }
});

db.createCollection("event_logs", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["event_id", "timestamp", "event_type"],
            properties: {
                event_id: { bsonType: "string" },
                timestamp: { bsonType: "date" },
                event_type: { bsonType: "string" },
                details: { bsonType: "object" }
            }
        }
    }
});

db.createCollection("support_tickets", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["ticket_id", "user_id", "status", "created_at", "updated_at"],
            properties: {
                ticket_id: { bsonType: "string" },
                user_id: { bsonType: "string" },
                status: { bsonType: "string" },
                issue_type: { bsonType: "string" },
                messages: { bsonType: "array" },
                created_at: { bsonType: "date" },
                updated_at: { bsonType: "date" }
            }
        }
    }
});

db.createCollection("user_recommendations", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["user_id", "recommended_products", "last_updated"],
            properties: {
                user_id: { bsonType: "string" },
                recommended_products: { bsonType: "array" },
                last_updated: { bsonType: "date" }
            }
        }
    }
});

db.createCollection("moderation_queue", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["review_id", "user_id", "product_id", "moderation_status", "submitted_at"],
            properties: {
                review_id: { bsonType: "string" },
                user_id: { bsonType: "string" },
                product_id: { bsonType: "string" },
                review_text: { bsonType: "string" },
                rating: { bsonType: "int" },
                moderation_status: { bsonType: "string" },
                flags: { bsonType: "array" },
                submitted_at: { bsonType: "date" }
            }
        }
    }
});


db.user_sessions.insertMany([
    {
        session_id: "sess_001",
        user_id: "user_123",
        start_time: new Date("2024-01-10T09:00:00Z"),
        end_time: new Date("2024-01-10T09:30:00Z"),
        pages_visited: ["/home", "/products", "/products/42", "/cart"],
        device: {"mobile": {}},
        actions: ["login", "view_product", "add_to_cart", "logout"]
    },
    {
        session_id: "sess_002",
        user_id: "user_123",
        start_time: new Date("2024-01-11T14:15:00Z"),
        end_time: new Date("2024-01-11T14:45:00Z"),
        pages_visited: ["/home", "/products", "/checkout"],
        device: {"desktop": {}},
        actions: ["login", "search", "checkout", "logout"]
    },
    {
        session_id: "sess_003",
        user_id: "user_456",
        start_time: new Date("2024-01-10T10:00:00Z"),
        end_time: new Date("2024-01-10T10:20:00Z"),
        pages_visited: ["/home", "/products", "/support"],
        device: {"tablet": {}},
        actions: ["login", "view_product", "contact_support"]
    },
     {
        session_id: "sess_004",
        user_id: "user_789",
        start_time: new Date("2024-02-01T10:30:00Z"),
        end_time: new Date("2024-02-01T11:15:00Z"),
        pages_visited: ["/home", "/products", "/products/15", "/cart", "/checkout"],
        device: {"mobile": {}},
        actions: ["login", "view_product", "add_to_cart", "checkout", "logout"]
    },
    {
        session_id: "sess_005",
        user_id: "user_123",
        start_time: new Date("2024-02-02T14:20:00Z"),
        end_time: new Date("2024-02-02T15:00:00Z"),
        pages_visited: ["/home", "/search", "/products/42", "/reviews"],
        device: {"desktop": {}},
        actions: ["login", "search", "view_product", "review", "logout"]
    },
    {
        session_id: "sess_006",
        user_id: "user_456",
        start_time: new Date("2024-02-03T09:45:00Z"),
        end_time: new Date("2024-02-03T10:20:00Z"),
        pages_visited: ["/home", "/profile", "/orders", "/support"],
        device: {"tablet": {}},
        actions: ["login", "view_profile", "check_orders", "contact_support"]
    },
    {
        session_id: "sess_007",
        user_id: "user_789",
        start_time: new Date("2024-02-05T16:10:00Z"),
        end_time: new Date("2024-02-05T16:45:00Z"),
        pages_visited: ["/home", "/products", "/products/33", "/wishlist"],
        device: {"mobile": {}},
        actions: ["login", "view_product", "wishlist_add", "share", "logout"]
    },
    {
        session_id: "sess_008",
        user_id: "user_234",
        start_time: new Date("2024-02-07T11:00:00Z"),
        end_time: new Date("2024-02-07T11:30:00Z"),
        pages_visited: ["/home", "/deals", "/products/27", "/cart"],
        device: {"desktop": {}},
        actions: ["login", "view_deals", "view_product", "add_to_cart"]
    },
    {
        session_id: "sess_009",
        user_id: "user_567",
        start_time: new Date("2024-02-08T13:15:00Z"),
        end_time: new Date("2024-02-08T14:00:00Z"),
        pages_visited: ["/home", "/categories", "/products/electronics", "/products/51", "/compare"],
        device: {"desktop": {}},
        actions: ["login", "browse_categories", "view_product", "compare_products"]
    },
    {
        session_id: "sess_010",
        user_id: "user_890",
        start_time: new Date("2024-02-10T08:30:00Z"),
        end_time: new Date("2024-02-10T09:10:00Z"),
        pages_visited: ["/home", "/new-arrivals", "/products/89", "/reviews", "/cart"],
        device: {"mobile": {}},
        actions: ["login", "view_new", "view_product", "read_reviews", "add_to_cart"]
    }
]);

db.event_logs.insertMany([
    {
        event_id: "evt_1001",
        timestamp: new Date("2024-01-10T09:05:20Z"),
        event_type: "click",
        details: { page: "/products/42" }
    },
    {
        event_id: "evt_1002",
        timestamp: new Date("2024-01-10T09:06:15Z"),
        event_type: "page_view",
        details: { page: "/cart" }
    },
    {
        event_id: "evt_1003",
        timestamp: new Date("2024-01-11T14:30:00Z"),
        event_type: "purchase",
        details: { order_id: "ord_789", amount: 150.00 }
    },
     {
        event_id: "evt_1004",
        timestamp: new Date("2024-02-01T10:35:00Z"),
        event_type: "search",
        details: { query: "iphone", results: 12 }
    },
    {
        event_id: "evt_1005",
        timestamp: new Date("2024-02-01T10:40:00Z"),
        event_type: "click",
        details: { page: "/products/15", element: "buy_button" }
    },
    {
        event_id: "evt_1006",
        timestamp: new Date("2024-02-01T10:45:00Z"),
        event_type: "add_to_cart",
        details: { product_id: "prod_015", quantity: 1, price: 299.99 }
    },
    {
        event_id: "evt_1007",
        timestamp: new Date("2024-02-02T14:25:00Z"),
        event_type: "search",
        details: { query: "wireless headphones", results: 8 }
    },
    {
        event_id: "evt_1008",
        timestamp: new Date("2024-02-02T14:35:00Z"),
        event_type: "review_submit",
        details: { product_id: "prod_042", rating: 5, title: "Отличный товар!" }
    },
    {
        event_id: "evt_1009",
        timestamp: new Date("2024-02-03T09:50:00Z"),
        event_type: "profile_update",
        details: { field: "phone", old: "+1234567890", new: "+0987654321" }
    },
    {
        event_id: "evt_1010",
        timestamp: new Date("2024-02-03T10:00:00Z"),
        event_type: "support_request",
        details: { ticket_id: "ticket_791", issue: "Не приходит подтверждение email" }
    },
    {
        event_id: "evt_1011",
        timestamp: new Date("2024-02-05T16:20:00Z"),
        event_type: "wishlist_add",
        details: { product_id: "prod_033", from_page: "/products/33" }
    },
    {
        event_id: "evt_1012",
        timestamp: new Date("2024-02-05T16:30:00Z"),
        event_type: "share",
        details: { product_id: "prod_033", platform: "facebook" }
    },
    {
        event_id: "evt_1013",
        timestamp: new Date("2024-02-07T11:10:00Z"),
        event_type: "price_alert",
        details: { product_id: "prod_027", old_price: 199.99, new_price: 149.99 }
    },
    {
        event_id: "evt_1014",
        timestamp: new Date("2024-02-07T11:20:00Z"),
        event_type: "cart_update",
        details: { action: "add", product_id: "prod_027", quantity: 2 }
    },
    {
        event_id: "evt_1015",
        timestamp: new Date("2024-02-08T13:30:00Z"),
        event_type: "product_compare",
        details: { products: ["prod_051", "prod_052", "prod_053"] }
    },
    {
        event_id: "evt_1016",
        timestamp: new Date("2024-02-08T13:45:00Z"),
        event_type: "filter_apply",
        details: { category: "electronics", price_range: "100-500", brand: "Samsung" }
    },
    {
        event_id: "evt_1017",
        timestamp: new Date("2024-02-10T08:45:00Z"),
        event_type: "new_arrivals_view",
        details: { count: 25, time_spent_seconds: 120 }
    },
    {
        event_id: "evt_1018",
        timestamp: new Date("2024-02-10T09:00:00Z"),
        event_type: "review_read",
        details: { product_id: "prod_089", reviews_read: 5 }
    }
]);

db.support_tickets.insertMany([
    {
        ticket_id: "ticket_789",
        user_id: "user_123",
        status: "open",
        issue_type: "payment",
        messages: [
            {
                sender: "user",
                message: "Не могу оплатить заказ.",
                timestamp: new Date("2024-01-09T12:00:00Z")
            },
            {
                sender: "support",
                message: "Пожалуйста, уточните способ оплаты.",
                timestamp: new Date("2024-01-09T13:00:00Z")
            }
        ],
        created_at: new Date("2024-01-09T11:55:00Z"),
        updated_at: new Date("2024-01-09T13:00:00Z")
    },
    {
        ticket_id: "ticket_790",
        user_id: "user_456",
        status: "closed",
        issue_type: "technical",
        messages: [
            {
                sender: "user",
                message: "Сайт не загружается",
                timestamp: new Date("2024-01-08T09:00:00Z")
            },
            {
                sender: "support",
                message: "Проблема решена, очистите кэш",
                timestamp: new Date("2024-01-08T10:30:00Z")
            }
        ],
        created_at: new Date("2024-01-08T09:00:00Z"),
        updated_at: new Date("2024-01-08T10:30:00Z")
    },
    {
        ticket_id: "ticket_791",
        user_id: "user_789",
        status: "open",
        issue_type: "delivery",
        messages: [
            {
                sender: "user",
                message: "Заказ не пришел в обещанный срок",
                timestamp: new Date("2024-02-01T09:00:00Z")
            },
            {
                sender: "support",
                message: "Проверяем статус доставки",
                timestamp: new Date("2024-02-01T10:30:00Z")
            }
        ],
        created_at: new Date("2024-02-01T09:00:00Z"),
        updated_at: new Date("2024-02-01T10:30:00Z")
    },
    {
        ticket_id: "ticket_792",
        user_id: "user_234",
        status: "closed",
        issue_type: "return",
        messages: [
            {
                sender: "user",
                message: "Хочу вернуть товар, как это сделать?",
                timestamp: new Date("2024-02-03T14:00:00Z")
            },
            {
                sender: "support",
                message: "Инструкция по возврату отправлена на email",
                timestamp: new Date("2024-02-03T15:15:00Z")
            },
            {
                sender: "user",
                message: "Спасибо, все получилось",
                timestamp: new Date("2024-02-04T10:00:00Z")
            }
        ],
        created_at: new Date("2024-02-03T14:00:00Z"),
        updated_at: new Date("2024-02-04T10:00:00Z")
    },
    {
        ticket_id: "ticket_793",
        user_id: "user_567",
        status: "pending",
        issue_type: "technical",
        messages: [
            {
                sender: "user",
                message: "Не работает фильтр по цене",
                timestamp: new Date("2024-02-05T11:30:00Z")
            },
            {
                sender: "support",
                message: "Какая у вас версия браузера?",
                timestamp: new Date("2024-02-05T12:45:00Z")
            }
        ],
        created_at: new Date("2024-02-05T11:30:00Z"),
        updated_at: new Date("2024-02-05T12:45:00Z")
    }
]);

db.user_recommendations.insertMany([
    {
        user_id: "user_123",
        recommended_products: ["prod_101", "prod_205", "prod_333"],
        last_updated: new Date("2024-01-10T08:00:00Z")
    },
    {
        user_id: "user_456",
        recommended_products: ["prod_102", "prod_206", "prod_334", "prod_401"],
        last_updated: new Date("2024-01-10T08:00:00Z")
    },
     {
        user_id: "user_789",
        recommended_products: ["prod_015", "prod_033", "prod_089", "prod_102", "prod_156"],
        last_updated: new Date("2024-02-05T00:00:00Z")
    },
    {
        user_id: "user_234",
        recommended_products: ["prod_027", "prod_042", "prod_051", "prod_078"],
        last_updated: new Date("2024-02-07T00:00:00Z")
    },
    {
        user_id: "user_567",
        recommended_products: ["prod_051", "prod_052", "prod_053", "prod_089", "prod_091", "prod_102"],
        last_updated: new Date("2024-02-08T00:00:00Z")
    }
]);

db.moderation_queue.insertMany([
    {
        review_id: "rev_555",
        user_id: "user_123",
        product_id: "prod_101",
        review_text: "Отличный товар, работает как нужно!",
        rating: 5,
        moderation_status: "pending",
        flags: ["contains_images"],
        submitted_at: new Date("2024-01-08T10:20:00Z")
    },
    {
        review_id: "rev_556",
        user_id: "user_456",
        product_id: "prod_102",
        review_text: "Товар не соответствует описанию",
        rating: 2,
        moderation_status: "approved",
        flags: [],
        submitted_at: new Date("2024-01-07T15:30:00Z")
    },
     {
        review_id: "rev_557",
        user_id: "user_789",
        product_id: "prod_015",
        review_text: "Отличный смартфон, батарея держит 2 дня!",
        rating: 5,
        moderation_status: "pending",
        flags: ["contains_images"],
        submitted_at: new Date("2024-02-01T12:00:00Z")
    },
    {
        review_id: "rev_558",
        user_id: "user_234",
        product_id: "prod_027",
        review_text: "Цена хорошая, но качество так себе",
        rating: 3,
        moderation_status: "approved",
        flags: [],
        submitted_at: new Date("2024-02-07T14:30:00Z")
    },
    {
        review_id: "rev_559",
        user_id: "user_890",
        product_id: "prod_089",
        review_text: "Лучшая покупка в этом году! Всем рекомендую",
        rating: 5,
        moderation_status: "pending",
        flags: ["contains_links"],
        submitted_at: new Date("2024-02-10T10:15:00Z")
    }
]);


db.user_sessions.createIndex({ user_id: 1, start_time: -1 });
db.event_logs.createIndex({ timestamp: -1, event_type: 1 });
db.support_tickets.createIndex({ user_id: 1, status: 1 });
db.support_tickets.createIndex({ created_at: -1 });
db.moderation_queue.createIndex({ moderation_status: 1, submitted_at: -1 });