数据存储
1：文件存储
2: mongdb存储

db.createUser( {user: "root", pwd: "123456", roles: [ { role:"userAdminAnyDatabase", db: "admin" } ]})