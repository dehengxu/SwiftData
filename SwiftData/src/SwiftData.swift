//
// SwiftData.swift
//
// Copyright (c) 2015 Ryan Fowler
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.


import Foundation
import UIKit
import SQLite3

// MARK: - SwiftData
public struct SwiftData {

    var db: SQLiteDB

    // MARK: - Public SwiftData Functions

    // MARK: - Execute Statements

    // MARK: - Escaping Objects

    /**
     Escape an object to be inserted into a SQLite statement as a value

     NOTE: Supported object types are: String, Int, Double, Bool, NSData, NSDate, and nil. All other data types will return the String value "NULL", and a warning message will be printed.

     :param: obj  The value to be escaped

     :returns:    The escaped value as a String, ready to be inserted into a SQL statement. Note: Single quotes (') will be placed around the entire value, if necessary.
     */
    static func escapeValue(_ db: SQLiteDB = .sharedInstance, obj: AnyObject?) -> String {
        return db.escapeValue(obj: obj)
    }

    /**
     Escape a string to be inserted into a SQLite statement as an indentifier (e.g. table or column name)

     :param: obj  The identifier to be escaped. NOTE: This object must be of type String.

     :returns:    The escaped identifier as a String, ready to be inserted into a SQL statement. Note: Double quotes (") will be placed around the entire identifier.
     */
    static func escapeIdentifier(_ db: SQLiteDB = .sharedInstance, obj: String) -> String {
        return db.escapeIdentifier(obj: obj)
    }


    // MARK: - Tables

    // MARK: - Misc

    // MARK: - Indexes

    // MARK: - Transactions and Savepoints

    // MARK: - SQLiteDB Class
    //NOTE: remove private level
    class SQLiteDB {

        // Singletone instance used in static functions, you can create your own instance also
        static let sharedInstance: SQLiteDB = SQLiteDB()

        //MARK: - Instance members

        var sqliteDB: OpaquePointer? = nil
        var dbPath = SQLiteDB.createPath()
        var inTransaction = false
        var isConnected = false
        var openWithFlags = false
        var savepointsOpen = 0
        //let queue = dispatch_queue_create("SwiftData.DatabaseQueue", DISPATCH_QUEUE_SERIAL)
        let queue = DispatchQueue(label: "SwiftData.DatabaseQueue")

        init(_ dbPath: String? = nil) {
            if let _ = dbPath {
                self.dbPath = dbPath!
            }
		}

        deinit {
            if self.openWithFlags {
                try? self.closeCustomConnection()
            } else {
                self.close()
            }
        }

        // MARK: - Database Handling Functions

		/// Open db
		/// - Parameter flags: SQLITE_OPEN_FLAGS
		/// - Returns: Void
		public func open(_ flags: [Int32] = [SQLITE_OPEN_CREATE, SQLITE_OPEN_READWRITE]) throws -> Void {

			if inTransaction || openWithFlags || savepointsOpen > 0 {
				return
			}
			if sqliteDB != nil || isConnected {
				return
			}
			var inFlags: Int32 = 0
			for flag in flags {
				inFlags |= flag
			}
			// SQLITE_OPEN_CREATE, SQLITE_OPEN_READWRITE
			let status = sqlite3_open_v2(dbPath.cString(using: .utf8)!, &sqliteDB, inFlags, nil)
			if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
				print("SwiftData Error -> During: Opening Database")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
				if let sqliteDB = self.sqliteDB {
					let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
					print("                -> Details: \(errMsg)")
				}
#endif
                throw SDError.error(code: status)
			}
			isConnected = true
			return
		}

        //open a connection to the sqlite3 database
        func open() throws -> Void {

            if inTransaction || openWithFlags || savepointsOpen > 0 {
                return
            }
            if sqliteDB != nil || isConnected {
                return
            }
            let status = sqlite3_open(dbPath.cString(using: .utf8)!, &sqliteDB)
            if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
#endif
                throw SD.SDError.error(code: status)
            }
            isConnected = true

        }

        //open a connection to the sqlite3 database with flags
        func openWithFlags(flags: Int32) throws -> Void {

            if inTransaction {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 302 - Cannot open a custom connection inside a transaction")
#endif
                throw SD.SDError.error(code: 302)
            }
            if openWithFlags {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 301 - A custom connection is already open")
#endif
                throw SD.SDError.error(code: 301)
            }
            if savepointsOpen > 0 {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 303 - Cannot open a custom connection inside a savepoint")
#endif
                throw SD.SDError.error(code: 303)
            }
            if isConnected {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 301 - A custom connection is already open")
#endif
                throw SD.SDError.error(code: 301)
            }
            let status = sqlite3_open_v2(dbPath.cString(using: .utf8)!, &sqliteDB, flags, nil)
            if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
#endif
                throw SD.SDError.error(code: status)
            }
            isConnected = true
            openWithFlags = true

        }

        //close the connection to to the sqlite3 database
        public func close() {

            if inTransaction || openWithFlags || savepointsOpen > 0 {
                return
            }
            if sqliteDB == nil || !isConnected {
                return
            }
            let status = sqlite3_close(sqliteDB)
            if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Closing Database")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
#endif
            }
            sqliteDB = nil
            isConnected = false

        }

        //close a custom connection to the sqlite3 database
        public func closeCustomConnection() throws -> Void {

            if inTransaction {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 305 - Cannot close a custom connection inside a transaction")
#endif
                throw SD.SDError.SQLITE(code: 305)
            }
            if savepointsOpen > 0 {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 306 - Cannot close a custom connection inside a savepoint")
#endif
				throw SD.SDError.SQLITE(code: 306)
            }
            if !openWithFlags {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 304 - A custom connection is not currently open")
#endif
				throw SD.SDError.SQLITE(code: 304)
            }
            let status = sqlite3_close(sqliteDB)
            sqliteDB = nil
            isConnected = false
            openWithFlags = false
            if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Closing Database with Flags")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
#endif
                throw SD.SDError.error(code: status)
            }

        }

        //create the database path, in library for some privacy
        class func createPath() -> String {

            let docsPath = NSSearchPathForDirectoriesInDomains(.libraryDirectory, .userDomainMask, true)[0] as String
            let databaseStr = "SwiftData.sqlite"
            let dbPath = docsPath.appending("/\(databaseStr)")
            return dbPath

        }

        //begin a transaction
        public func beginTransaction() throws -> Void {

            if savepointsOpen > 0 {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Beginning Transaction")
                print("                -> Code: 501 - Cannot begin a transaction within a savepoint")
#endif
				throw SD.SDError.SQLITE(code: 501)
            }
            if inTransaction {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: Beginning Transaction")
                print("                -> Code: 502 - Cannot begin a transaction within another transaction")
#endif
				throw SD.SDError.SQLITE(code: 502)
            }
			do {
				try executeChange(sqlStr: "BEGIN EXCLUSIVE")
			} catch {
				throw error
			}
            inTransaction = true

        }

        //rollback a transaction
        public func rollbackTransaction() throws -> Void {

//            let error = executeChange(sqlStr: "ROLLBACK")
//            inTransaction = false
//            return error

			defer {
				inTransaction = false
			}
			do {
				try executeChange(sqlStr: "ROLLBACK")
			} catch {
				throw error
			}

        }

        //commit a transaction
        public func commitTransaction() throws -> Void {

//            let error = executeChange(sqlStr: "COMMIT")
//            inTransaction = false
//            if let err = error {
//                _ = rollbackTransaction()
//                return err
//            }
//            return nil

			defer {
				inTransaction = false
			}
			do {
				try executeChange(sqlStr: "COMMIT")
			} catch {
				do {
					try rollbackTransaction()
				} catch {
				}
				throw error
			}

        }

        //begin a savepoint
        public func beginSavepoint() throws -> Void {

//            if let error = executeChange(sqlStr: "SAVEPOINT 'savepoint\(savepointsOpen + 1)'") {
//                return error
//            }
//            savepointsOpen += 1
//            return nil

			do {
				try executeChange(sqlStr: "SAVEPOINT 'savepoint\(savepointsOpen + 1)'")
				savepointsOpen += 1
			} catch {
				throw error
			}

        }

        //rollback a savepoint
        public func rollbackSavepoint() throws -> Void {
            //return executeChange(sqlStr: "ROLLBACK TO 'savepoint\(savepointsOpen)'")
			do {
				try executeChange(sqlStr: "ROLLBACK TO 'savepoint\(savepointsOpen)'")
			} catch {
				throw error
			}
        }

        //release a savepoint
        public func releaseSavepoint() throws -> Void {

//            let error = executeChange(sqlStr: "RELEASE 'savepoint\(savepointsOpen)'")
//            savepointsOpen -= 1
//            return error

			do {
				try executeChange(sqlStr: "RELEASE 'savepoint\(savepointsOpen)'")
			} catch {
				throw error
			}

        }

        //get last inserted row id
        public func lastInsertedRowID() -> Int {
            let id = sqlite3_last_insert_rowid(sqliteDB)
            return Int(id)
        }

        //number of rows changed by last update
        public func numberOfRowsModified() -> Int {
            return Int(sqlite3_changes(sqliteDB))
        }

        //return value of column
        func getColumnValue(statement: OpaquePointer, index: Int32, type: String) -> AnyObject? {

            switch type {
                case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "UNSIGNED BIG INT", "INT2", "INT8":
                    if sqlite3_column_type(statement, index) == SQLITE_NULL {
                        return nil
                    }
                    return Int(sqlite3_column_int(statement, index)) as AnyObject
                case "CHARACTER(20)", "VARCHAR(255)", "VARYING CHARACTER(255)", "NCHAR(55)", "NATIVE CHARACTER", "NVARCHAR(100)", "TEXT", "CLOB":
                    if let text = UnsafePointer<UInt8>(sqlite3_column_text(statement, index)) {
                        return String(cString: text) as AnyObject
                    }
                    return nil
                case "BLOB", "NONE":
                    let blob = sqlite3_column_blob(statement, index)
                    if blob != nil {
                        let size = sqlite3_column_bytes(statement, index)
                        return NSData(bytes: blob, length: Int(size))
                    }
                    return nil
                case "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT", "NUMERIC", "DECIMAL(10,5)":
                    if sqlite3_column_type(statement, index) == SQLITE_NULL {
                        return nil
                    }
                    return Double(sqlite3_column_double(statement, index)) as AnyObject
                case "BOOLEAN":
                    if sqlite3_column_type(statement, index) == SQLITE_NULL {
                        return nil
                    }
                    return (sqlite3_column_int(statement, index) != 0) as AnyObject
                case "DATE", "DATETIME":
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
                    if let text = UnsafePointer<UInt8>(sqlite3_column_text(statement, index)) {
                        let string = String(cString: text)
                        return dateFormatter.date(from: string) as AnyObject
                    }
                    print("SwiftData Warning -> The text date at column: \(index) could not be cast as a String, returning nil")
                    return nil
                default:
                    print("SwiftData Warning -> Column: \(index) is of an unrecognized type, returning nil")
                    return nil
            }

        }


        // MARK: SQLite Execution Functions

        //execute a SQLite update from a SQL String
        public func executeChange(sqlStr: String, withArgs: [AnyObject]? = nil) throws -> Void {

            var sql = sqlStr
            if let args = withArgs {
				do {
					sql = try bind(objects: args, toSQL: sql)
				} catch {
					throw error
				}
            }
            var pStmt: OpaquePointer? = nil
            var status = sqlite3_prepare_v2(self.sqliteDB, sql, -1, &pStmt, nil)
            if status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: SQL Prepare")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                print("                -> sql: \(sql)")
                if let sqliteDB = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
#endif
                sqlite3_finalize(pStmt)
				throw SD.SDError.error(code: status)
            }
            status = sqlite3_step(pStmt)
            if status != SQLITE_DONE && status != SQLITE_OK {
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: SQL Step")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let db = sqlite3_errmsg(self.sqliteDB) {
                    let errMsg = String(cString: db)
                    print("                -> Details: \(errMsg)")
                }
#endif
                sqlite3_finalize(pStmt)
				throw SD.SDError.error(code: status)
            }
            sqlite3_finalize(pStmt)
        }

        //execute a SQLite query from a SQL String
        public func executeQuery(sqlStr: String, withArgs: [AnyObject]? = nil) throws -> [SDRow] {

            var resultSet = [SDRow]()
            var sql = sqlStr
            if let args = withArgs {
				do {
					sql = try bind(objects: args, toSQL: sql)
				} catch {
					throw error
				}
            }
            var pStmt: OpaquePointer? = nil
            var status = sqlite3_prepare_v2(self.sqliteDB, sql, -1, &pStmt, nil)
            if status != SQLITE_OK {
                let error: SD.SDError = .error(code: status)
#if SWIFT_DATA_DEBUG
                print("SwiftData Error -> During: SQL Prepare")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let db = self.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(db))
                    print("                -> Details: \(errMsg)")
                }
#endif
                sqlite3_finalize(pStmt)
				throw error
            }
            var columnCount: Int32 = 0
            var next = true
            while next {
                status = sqlite3_step(pStmt)
                if status == SQLITE_ROW {
                    columnCount = sqlite3_column_count(pStmt)
                    var row = SDRow()
                    var i: Int32 = 0
                    while i < columnCount {
                        let columnName = String(cString: sqlite3_column_name(pStmt, i))
						let columnType = "\(String(describing: sqlite3_column_decltype(pStmt, i)))".uppercased()
                        if !columnType.isEmpty {
                            if let columnValue: AnyObject = getColumnValue(statement: pStmt!, index: i, type: columnType) {
                                row[columnName] = SDColumn(obj: columnValue)
                            }
                        } else {
                            var columnType = ""
                            switch sqlite3_column_type(pStmt, i) {
                                case SQLITE_INTEGER:
                                    columnType = "INTEGER"
                                case SQLITE_FLOAT:
                                    columnType = "FLOAT"
                                case SQLITE_TEXT:
                                    columnType = "TEXT"
                                case SQLITE3_TEXT:
                                    columnType = "TEXT"
                                case SQLITE_BLOB:
                                    columnType = "BLOB"
                                case SQLITE_NULL:
                                    columnType = "NULL"
                                default:
                                    columnType = "NULL"
                            }
                            if let columnValue: AnyObject = getColumnValue(statement: pStmt!, index: i, type: columnType) {
                                row[columnName] = SDColumn(obj: columnValue)
                            }
                        }

                        i += 1
                    }
                    resultSet.append(row)
                } else if status == SQLITE_DONE {
                    next = false
                } else {
                    let error: SD.SDError = .error(code: status)
#if SWIFT_DATA_DEBUG
                    print("SwiftData Error -> During: SQL Step")
					print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                    if let db = sqlite3_errmsg(self.sqliteDB) {
                        let errMsg = String(cString: db)
                        print("                -> Details: \(errMsg)")
                    }
#endif
                    sqlite3_finalize(pStmt)
					throw error
                    //return (resultSet, Int(status))
                }
            }
            sqlite3_finalize(pStmt)
            //return (resultSet, nil)
			return resultSet
        }

        // MARK: - Threading

        func putOnThread(task: () throws ->Void) throws {
            if self.inTransaction || self.savepointsOpen > 0 || self.openWithFlags {
                try task()
            } else {
                try self.queue.sync() {
                    try task()
                }
            }
        }

    }


    // MARK: - SDRow

    public struct SDRow {

        var values = [String: SDColumn]()
        public subscript(key: String) -> SDColumn? {
            get {
                return values[key]
            }
            set(newValue) {
                values[key] = newValue
            }
        }

    }


    // MARK: - SDColumn

    public struct SDColumn {

        var value: AnyObject
        init(obj: AnyObject) {
            value = obj
        }

        //return value by type
        /**
         Return the column value as a String
         :returns:  An Optional String corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as a String, or the value is NULL
         */
        public func asString() -> String? {
            return value as? String
        }

        /**
         Return the column value as an Int
         :returns:  An Optional Int corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as a Int, or the value is NULL
         */
        public func asInt() -> Int? {
            return value as? Int
        }

        /**
         Return the column value as a Double
         :returns:  An Optional Double corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as a Double, or the value is NULL
         */
        public func asDouble() -> Double? {
            return value as? Double
        }

        /**
         Return the column value as a Bool
         :returns:  An Optional Bool corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as a Bool, or the value is NULL
         */
        public func asBool() -> Bool? {
            return value as? Bool
        }

        /**
         Return the column value as NSData
         :returns:  An Optional NSData object corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as NSData, or the value is NULL
         */
        public func asData() -> NSData? {
            return value as? NSData
        }

        /**
         Return the column value as an NSDate
         :returns:  An Optional NSDate corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as an NSDate, or the value is NULL
         */
        public func asDate() -> NSDate? {
            return value as? NSDate
        }

        /**
         Return the column value as an AnyObject
         :returns:  An Optional AnyObject corresponding to the apprioriate column value. Will be nil if: the column name does not exist, the value cannot be cast as an AnyObject, or the value is NULL
         */
        public func asAnyObject() -> AnyObject? {
            return value
        }

        /**
         Return the column value path as a UIImage
         :returns:  An Optional UIImage corresponding to the path of the apprioriate column value. Will be nil if: the column name does not exist, the value of the specified path cannot be cast as a UIImage, or the value is NULL
         */
        public func asUIImage() -> UIImage? {

            if let path = value as? String{
                let docsPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0] as String
                let imageDirPath = docsPath.appending("/SwiftDataImages")
                let fullPath = imageDirPath.appending("/\(path)")
                if !FileManager.default.fileExists(atPath: fullPath) {
#if SWIFT_DATA_DEBUG
                    print("SwiftData Error -> Invalid image ID provided")
#endif
                    return nil
                }
                if let imageAsData = NSData(contentsOfFile: fullPath) {
                    return UIImage(data: imageAsData as Data)
                }
            }
            return nil

        }

    }

}

// MARK: - Threading
extension SwiftData {
    //NOTE: move to SQLiteDB
//    private static func putOnThread(_ db: SQLiteDB = .sharedInstance, task: () throws ->Void) throws {
//        try db.putOnThread(task: task)
//        //        if SQLiteDB.sharedInstance.inTransaction || SQLiteDB.sharedInstance.savepointsOpen > 0 || SQLiteDB.sharedInstance.openWithFlags {
//        //            try task()
//        //        } else {
//        //            try SQLiteDB.sharedInstance.queue.sync() {
//        //                try task()
//        //            }
//        //        }
//    }

}


// MARK: - Escaping And Binding Functions
extension SwiftData.SQLiteDB {

	func bind(objects: [AnyObject], toSQL sql: String) throws -> String {

		var newSql = ""
		var bindIndex = 0
		var i = false
		for char in sql {
			if char == "?" {
				if bindIndex > objects.count - 1 {
					let error = SD.SDError.SQLITE(code: 201)
#if SWIFT_DATA_DEBUG
					print("SwiftData Error -> During: Object Binding")
					print("                -> Code: 201 - \(error.message())")
#endif
					throw error
				}
				var obj = ""
				if i {
					if let str = objects[bindIndex] as? String {
						obj = escapeIdentifier(obj: str)
					} else {
						let error = SD.SDError.SQLITE(code: 203)
#if SWIFT_DATA_DEBUG
						print("SwiftData Error -> During: Object Binding")
						print("                -> Code: 203 - \(error.message()) at array location: \(bindIndex)")
#endif
						throw error
					}
					//WARN: newSql.endIndex.predecessor() -> newSql.endIndex
					//newSql = newSql.substring(to: newSql.endIndex)
					//let range: Range<String.Index> = newSql.startIndex..<newSql.endIndex
					newSql = String(newSql[newSql.startIndex..<newSql.endIndex])
				} else {
					obj = escapeValue(obj: objects[bindIndex])
				}
				newSql += obj
				bindIndex += 1
			} else {
				newSql.append(char)
			}
			if char == "i" {
				i = true
			} else if i {
				i = false
			}
		}
		if bindIndex != objects.count {
			let error = SD.SDError.SQLITE(code: 202)
#if SWIFT_DATA_DEBUG
			print("SwiftData Error -> During: Object Binding")
			print("                -> Code: 202 - \(error.message())")
#endif
			throw error
		}
		return newSql
	}

//    func bind(objects: [AnyObject], toSQL sql: String) -> (string: String, error: Int?) {
//
//        var newSql = ""
//        var bindIndex = 0
//        var i = false
//        for char in sql {
//            if char == "?" {
//                if bindIndex > objects.count - 1 {
//                    print("SwiftData Error -> During: Object Binding")
//                    print("                -> Code: 201 - Not enough objects to bind provided")
//                    return ("", 201)
//                }
//                var obj = ""
//                if i {
//                    if let str = objects[bindIndex] as? String {
//                        obj = escapeIdentifier(obj: str)
//                    } else {
//                        print("SwiftData Error -> During: Object Binding")
//                        print("                -> Code: 203 - Object to bind as identifier must be a String at array location: \(bindIndex)")
//                        return ("", 203)
//                    }
//                    //WARN: newSql.endIndex.predecessor() -> newSql.endIndex 可能会报错
//                    //newSql = newSql.substring(to: newSql.endIndex)
//                    //let range: Range<String.Index> = newSql.startIndex..<newSql.endIndex
//                    newSql = String(newSql[newSql.startIndex..<newSql.endIndex])
//                } else {
//                    obj = escapeValue(obj: objects[bindIndex])
//                }
//                newSql += obj
//                bindIndex += 1
//            } else {
//                newSql.append(char)
//            }
//            if char == "i" {
//                i = true
//            } else if i {
//                i = false
//            }
//        }
//        if bindIndex != objects.count {
//            print("SwiftData Error -> During: Object Binding")
//            print("                -> Code: 202 - Too many objects to bind provided")
//            return ("", 202)
//        }
//        return (newSql, nil)
//
//    }

    //return escaped String value of AnyObject
    func escapeValue(obj: AnyObject?) -> String {

        if let obj: AnyObject = obj {
            if obj is String {
                return "'\(escapeStringValue(str: obj as! String))'"
            }
            if obj is Double || obj is Int {
                return "\(obj)"
            }
            if obj is Bool {
                if obj as! Bool {
                    return "1"
                } else {
                    return "0"
                }
            }
            if obj is NSData {
                let str = "\(obj)"
                var newStr = ""
                for char in str {
                    if char != "<" && char != ">" && char != " " {
                        newStr.append(char)
                    }
                }
                return "X'\(newStr)'"
            }
            if obj is NSDate {
                let dateFormatter = DateFormatter()
                dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
                let str = dateFormatter.string(from: (obj as! Date))
                return "\(escapeValue(obj: str as AnyObject))"
            }
            if obj is UIImage {
                if let imageID = SD.saveUIImage(image: obj as! UIImage) {
                    return "'\(escapeStringValue(str: imageID))'"
                }
                print("SwiftData Warning -> Cannot save image, NULL will be inserted into the database")
                return "NULL"
            }
            print("SwiftData Warning -> Object \"\(obj)\" is not a supported type and will be inserted into the database as NULL")
            return "NULL"
        } else {
            return "NULL"
        }

    }

    //return escaped String identifier
    func escapeIdentifier(obj: String) -> String {
        return "\"\(escapeStringIdentifier(str: obj))\""
    }


    //escape string
    func escapeStringValue(str: String) -> String {
        var escapedStr = ""
        for char in str {
            if char == "'" {
                escapedStr += "'"
            }
            escapedStr.append(char)
        }
        return escapedStr
    }

    //escape string
    func escapeStringIdentifier(str: String) -> String {
        var escapedStr = ""
        for char in str {
            if char == "\"" {
                escapedStr += "\""
            }
            escapedStr.append(char)
        }
        return escapedStr
    }

}


// MARK: - SQL Creation Functions
extension SwiftData {

    /**
     Column Data Types

     :param:  StringVal   A column with type String, corresponds to SQLite type "TEXT"
     :param:  IntVal      A column with type Int, corresponds to SQLite type "INTEGER"
     :param:  DoubleVal   A column with type Double, corresponds to SQLite type "DOUBLE"
     :param:  BoolVal     A column with type Bool, corresponds to SQLite type "BOOLEAN"
     :param:  DataVal     A column with type NSdata, corresponds to SQLite type "BLOB"
     :param:  DateVal     A column with type NSDate, corresponds to SQLite type "DATE"
     :param:  UIImageVal  A column with type String (the path value of saved UIImage), corresponds to SQLite type "TEXT"
     */
    public enum DataType {

        case StringVal
        case IntVal
        case DoubleVal
        case BoolVal
        case DataVal
        case DateVal
        case UIImageVal

        //NOTE: private to fileprivate
        fileprivate func toSQL() -> String {

            switch self {
                case .StringVal, .UIImageVal:
                    return "TEXT"
                case .IntVal:
                    return "INTEGER"
                case .DoubleVal:
                    return "DOUBLE"
                case .BoolVal:
                    return "BOOLEAN"
                case .DataVal:
                    return "BLOB"
                case .DateVal:
                    return "DATE"
            }
        }

    }

    /**
     Flags for custom connection to the SQLite database

     :param:  ReadOnly         Opens the SQLite database with the flag "SQLITE_OPEN_READONLY"
     :param:  ReadWrite        Opens the SQLite database with the flag "SQLITE_OPEN_READWRITE"
     :param:  ReadWriteCreate  Opens the SQLite database with the flag "SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE"
     */
    public enum Flags {

        case ReadOnly
        case ReadWrite
        case ReadWriteCreate

        //NOTE: origin access level is private. dehengxu@outlook.com
        func toSQL() -> Int32 {

            switch self {
                case .ReadOnly:
                    return SQLITE_OPEN_READONLY
                case .ReadWrite:
                    return SQLITE_OPEN_READWRITE
                case .ReadWriteCreate:
                    return SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            }

        }

    }

}


extension SwiftData.SQLiteDB {

	typealias ColumnTypes = [String: SwiftData.DataType]

    //create a table
    func createSQLTable(table: String, withColumnsAndTypes values: [String: SwiftData.DataType]) throws -> Void {

        var sqlStr = "CREATE TABLE IF NOT EXISTS \(table) (ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        var firstRun = true
        for value in values {
            if firstRun {
                sqlStr += "\(escapeIdentifier(obj: value.0)) \(value.1.toSQL())"
                firstRun = false
            } else {
                sqlStr += ", \(escapeIdentifier(obj: value.0)) \(value.1.toSQL())"
            }
        }
        sqlStr += ");"
        try executeChange(sqlStr: sqlStr)

    }

    //delete a table
    func deleteSQLTable(table: String) throws -> Void {
        let sqlStr = "DROP TABLE \(table)"
        try executeChange(sqlStr: sqlStr)
    }

    //get existing table names
    func existingTables() throws -> [String] {
        let sqlStr = "SELECT name FROM sqlite_master WHERE type = 'table'"
        var tableArr = [String]()

        let results = try executeQuery(sqlStr: sqlStr)
        for row in results {
            if let table = row["name"]?.asString() {
                tableArr.append(table)
            } else {
                //print("SwiftData Error -> During: Finding Existing Tables")
                //print("                -> Code: 403 - Error extracting table names from sqlite_master")
				throw SD.SDError.SQLITE(code: 403)
            }
        }
		return tableArr
    }

    //create an index
    func createIndex(name: String, columns: [String], table: String, unique: Bool) throws -> Void {

        if columns.count < 1 {
            print("SwiftData Error -> During: Creating Index")
            print("                -> Code: 401 - At least one column name must be provided")
			throw SD.SDError.SQLITE(code: 401)
        }
        var sqlStr = ""
        if unique {
            sqlStr = "CREATE UNIQUE INDEX \(name) ON \(table) ("
        } else {
            sqlStr = "CREATE INDEX \(name) ON \(table) ("
        }
        var firstRun = true
        for column in columns {
            if firstRun {
                sqlStr += column
                firstRun = false
            } else {
                sqlStr += ", \(column)"
            }
        }
        sqlStr += ")"
        try executeChange(sqlStr: sqlStr)
    }

    //remove an index
    func removeIndex(name: String) throws -> Void {
        let sqlStr = "DROP INDEX \(name)"
        try executeChange(sqlStr: sqlStr)
    }

    //obtain list of existing indexes
    func existingIndexes() throws -> [String] {

        let sqlStr = "SELECT name FROM sqlite_master WHERE type = 'index'"
        var indexArr = [String]()
        let results = try executeQuery(sqlStr: sqlStr)
        for res in results {
            if let index = res["name"]?.asString() {
                indexArr.append(index)
            } else {
                print("SwiftData Error -> During: Finding Existing Indexes")
                print("                -> Code: 402 - Error extracting index names from sqlite_master")
                print("Error finding existing indexes -> Error extracting index names from sqlite_master")
				throw SD.SDError.SQLITE(code: 402)
            }
        }
        return indexArr
    }

    //obtain list of existing indexes for a specific table
    func existingIndexesForTable(table: String) throws -> [String] {

        let sqlStr = "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '\(table)'"
        var indexArr = [String]()
        let results = try executeQuery(sqlStr: sqlStr)
        for res in results {
            if let index = res["name"]?.asString() {
                indexArr.append(index)
            } else {
                print("SwiftData Error -> During: Finding Existing Indexes for a Table")
                print("                -> Code: 402 - Error extracting index names from sqlite_master")
				throw SD.SDError.SQLITE(code: 402)
            }
        }
        return indexArr
    }

}

public typealias SD = SwiftData
//public typealias SDB = SwiftData.SQLiteDB
