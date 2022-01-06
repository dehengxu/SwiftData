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


    // MARK: - Public SwiftData Functions


    // MARK: - Execute Statements

    /**
     Execute a non-query SQL statement (e.g. INSERT, UPDATE, DELETE, etc.)

     This function will execute the provided SQL and return an Int with the error code, or nil if there was no error.
     It is recommended to always verify that the return value is nil to ensure that the operation was successful.

     Possible errors returned by this function are:

     - SQLite errors (0 - 101)

     :param: sqlStr  The non-query string of SQL to be executed (INSERT, UPDATE, DELETE, etc.)

     :returns:       An Int with the error code, or nil if there was no error
     */
    public static func executeChange(sqlStr: String) throws -> Void {

        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            try SQLiteDB.sharedInstance.executeChange(sqlStr: sqlStr)
        }
        try putOnThread(task: task)

    }

    /**
     Execute a non-query SQL statement (e.g. INSERT, UPDATE, DELETE, etc.) along with arguments to be bound to the characters "?" (for values) and "i?" (for identifiers e.g. table or column names).

     The objects in the provided array of arguments will be bound, in order, to the "i?" and "?" characters in the SQL string.
     The quantity of "i?"s and "?"s in the SQL string must be equal to the quantity of arguments provided.
     Objects that are to bind as an identifier ("i?") must be of type String.
     Identifiers should be bound and escaped if provided by the user.
     If "nil" is provided as an argument, the NULL value will be bound to the appropriate value in the SQL string.
     For more information on how the objects will be escaped, refer to the functions "escapeValue()" and "escapeIdentifier()".
     Note that the "escapeValue()" and "escapeIdentifier()" include the necessary quotations ' ' or " " to the arguments when being bound to the SQL.
     It is recommended to always verify that the return value is nil to ensure that the operation was successful.

     Possible errors returned by this function are:

     - SQLite errors (0 - 101)
     - binding errors (201 - 203)

     :param: sqlStr    The non-query string of SQL to be executed (INSERT, UPDATE, DELETE, etc.)
     :param: withArgs  An array of objects to bind to the "?" and "i?" characters in the sqlStr

     :returns:         An Int with the error code, or nil if there was no error
     */
    public static func executeChange(sqlStr: String, withArgs: [AnyObject]) throws -> Void {

        let task: () throws ->Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            try SQLiteDB.sharedInstance.executeChange(sqlStr: sqlStr, withArgs: withArgs)
        }
        try putOnThread(task: task)

    }

    /**
     Execute multiple SQL statements (non-queries e.g. INSERT, UPDATE, DELETE, etc.)
     This function will execute each SQL statment in the provided array, in order, and return an Int with the error code, or nil if there was no error.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param: sqlArr  An array of non-query strings of SQL to be executed (INSERT, UPDATE, DELETE, etc.)

     :returns:       An Int with the error code, or nil if there was no error
     */
#if false
    public static func executeMultipleChanges(sqlArr: [String]) -> Int? {

        var error: Int? = nil
        let task: ()->Void = {
            if let err = SQLiteDB.sharedInstance.open() {
                error = err
                return
            }
            for sqlStr in sqlArr {
                if let err = SQLiteDB.sharedInstance.executeChange(sqlStr: sqlStr) {
                    SQLiteDB.sharedInstance.close()
                    if let index = find(sqlArr, sqlStr) {
                        print("Error occurred on array item: \(index) -> \"\(sqlStr)\"")
                    }
                    error = err
                    return
                }
            }
            SQLiteDB.sharedInstance.close()
        }
        putOnThread(task: task)
        return error

    }
#endif

    /**
     Execute a SQLite query statement (e.g. SELECT)
     This function will execute the provided SQL and return a tuple of:
     - an Array of SDRow objects
     - an Int with the error code, or nil if there was no error

     The value for each column in an SDRow can be obtained using the column name in the subscript format similar to a Dictionary, along with the function to obtain the value in the appropriate type (.asString(), .asDate(), .asData(), .asInt(), .asDouble(), and .asBool()).
     Without the function call to return a specific type, the SDRow will return an object with type AnyObject.
     Note: NULL values in the SQLite database will be returned as 'nil'.

     Possible errors returned by this function are:

     - SQLite errors (0 - 101)

     :param: sqlStr  The query String of SQL to be executed (e.g. SELECT)

     :returns:       A tuple containing an Array of "SDRow"s, and an Int with the error code or nil if there was no error
     */
    public static func executeQuery(sqlStr: String) throws -> [SDRow] {

        var result = [SDRow] ()
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            result = try SQLiteDB.sharedInstance.executeQuery(sqlStr: sqlStr)
        }
        try putOnThread(task: task)
        return result

    }

    /**
     Execute a SQL query statement (e.g. SELECT) with arguments to be bound to the characters "?" (for values) and "i?" (for identifiers e.g. table or column names).

     See the "executeChange(sqlStr: String, withArgs: [AnyObject?])" function for more information on the arguments provided and binding.
     See the "executeQuery(sqlStr: String)"  function for more information on the return value.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - binding errors (201 - 203)
     :param: sqlStr    The query String of SQL to be executed (e.g. SELECT)
     :param: withArgs  An array of objects that will be bound, in order, to the characters "?" (for values) and "i?" (for identifiers, e.g. table or column names) in the sqlStr.

     :returns:       A tuple containing an Array of "SDRow"s, and an Int with the error code or nil if there was no error
     */
    public static func executeQuery(sqlStr: String, withArgs: [AnyObject]) throws -> [SDRow] {

        var result = [SDRow] ()
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            result = try SQLiteDB.sharedInstance.executeQuery(sqlStr: sqlStr, withArgs: withArgs)
        }
        try putOnThread(task: task)
        return result

    }

    /**
     Execute functions in a closure on a single custom connection


     Note: This function cannot be nested within itself, or inside a transaction/savepoint.
     Possible errors returned by this function are:
     - custom connection errors (301 - 306)
     :param: flags    The custom flag associated with the connection. Can be either:
     - .ReadOnly
     - .ReadWrite
     - .ReadWriteCreate
     :param: closure  A closure containing functions that will be executed on the custom connection
     :returns:        An Int with the error code, or nil if there was no error
     */
    public static func executeWithConnection(flags: SD.Flags, closure: ()->Void) throws -> Void {

        let task: () throws -> Void = {
            if let err = SQLiteDB.sharedInstance.openWithFlags(flags: flags.toSQL()) {
				throw SD.SDError.SQLITE3(code: err)
            }
            closure()
            try SQLiteDB.sharedInstance.closeCustomConnection()
        }
        try putOnThread(task: task)

    }


    // MARK: - Escaping Objects

    /**
     Escape an object to be inserted into a SQLite statement as a value

     NOTE: Supported object types are: String, Int, Double, Bool, NSData, NSDate, and nil. All other data types will return the String value "NULL", and a warning message will be printed.

     :param: obj  The value to be escaped

     :returns:    The escaped value as a String, ready to be inserted into a SQL statement. Note: Single quotes (') will be placed around the entire value, if necessary.
     */
    public static func escapeValue(obj: AnyObject?) -> String {
        return SQLiteDB.sharedInstance.escapeValue(obj: obj)
    }

    /**
     Escape a string to be inserted into a SQLite statement as an indentifier (e.g. table or column name)

     :param: obj  The identifier to be escaped. NOTE: This object must be of type String.

     :returns:    The escaped identifier as a String, ready to be inserted into a SQL statement. Note: Double quotes (") will be placed around the entire identifier.
     */
    public static func escapeIdentifier(obj: String) -> String {
        return SQLiteDB.sharedInstance.escapeIdentifier(obj: obj)
    }


    // MARK: - Tables

    /**
     Create A Table With The Provided Column Names and Types
     Note: The ID field is created automatically as "INTEGER PRIMARY KEY AUTOINCREMENT"
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param:  table                The table name to be created
     :param:  columnNamesAndTypes  A dictionary where the key = column name, and the value = data type

     :returns:                     An Int with the error code, or nil if there was no error
     */
    public static func createTable(table: String, withColumnNamesAndTypes values: [String: SwiftData.DataType]) throws -> Void {

        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            try SQLiteDB.sharedInstance.createSQLTable(table: table, withColumnsAndTypes: values)
        }
        try putOnThread(task: task)

    }

    /**
     Delete a SQLite table by name
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param:  table  The table name to be deleted

     :returns:       An Int with the error code, or nil if there was no error
     */
    public static func deleteTable(table: String) throws -> Void {

        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            try SQLiteDB.sharedInstance.deleteSQLTable(table: table)
        }
        try putOnThread(task: task)

    }

    /**
     Obtain a list of the existing SQLite table names
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Table query error (403)

     :returns:  A tuple containing an Array of all existing SQLite table names, and an Int with the error code or nil if there was no error
     */
    public static func existingTables() throws -> [String] {

        var result = [String] ()
        let task: () throws -> Void = {
            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
            }
            result = try SQLiteDB.sharedInstance.existingTables()
            SQLiteDB.sharedInstance.close()
        }
        try putOnThread(task: task)
        return result

    }


    // MARK: - Misc

    /**
     Obtain the error message relating to the provided error code
     :param: code  The error code provided
     :returns:     The error message relating to the provided error code
     */
    public static func errorMessageForCode(code: Int) -> String {
        return SwiftData.SDError.message(code: code)
    }

    /**
     Obtain the database path

     :returns:  The path to the SwiftData database
     */
    public static func databasePath() -> String {
        return SQLiteDB.sharedInstance.dbPath
    }

    /**
     Obtain the last inserted row id
     Note: Care should be taken when the database is being accessed from multiple threads. The value could possibly return the last inserted row ID for another operation if another thread executes after your intended operation but before this function call.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :returns:  A tuple of he ID of the last successfully inserted row's, and an Int of the error code or nil if there was no error
     */
    public static func lastInsertedRowID() throws -> Int {

        var result = 0
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            result = SQLiteDB.sharedInstance.lastInsertedRowID()
        }
        try putOnThread(task: task)
        return result

    }

    /**
     Obtain the number of rows modified by the most recently completed SQLite statement (INSERT, UPDATE, or DELETE)
     Note: Care should be taken when the database is being accessed from multiple threads. The value could possibly return the number of rows modified for another operation if another thread executes after your intended operation but before this function call.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :returns:  A tuple of the number of rows modified by the most recently completed SQLite statement, and an Int with the error code or nil if there was no error
     */
    public static func numberOfRowsModified() throws -> Int {

        var result = 0
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
			if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            result = try SQLiteDB.sharedInstance.numberOfRowsModified()
        }
        try putOnThread(task: task)
        return result

    }


    // MARK: - Indexes

    /**
     Create a SQLite index on the specified table and column(s)
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Index error (401)

     :param: name       The index name that is being created
     :param: onColumns  An array of column names that the index will be applied to (must be one column or greater)
     :param: inTable    The table name where the index is being created
     :param: isUnique   True if the index should be unique, false if it should not be unique (defaults to false)

     :returns:          An Int with the error code, or nil if there was no error
     */
    public static func createIndex(name: String, onColumns: [String], inTable: String, isUnique: Bool = false) throws -> Void {

        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
            }
            try SQLiteDB.sharedInstance.createIndex(name: name, columns: onColumns, table: inTable, unique: isUnique)
        }
        try putOnThread(task: task)

    }

    /**
     Remove a SQLite index by its name
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param: indexName  The name of the index to be removed

     :returns:          An Int with the error code, or nil if there was no error
     */
    public static func removeIndex(indexName: String) throws -> Void {

        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
			}
            try SQLiteDB.sharedInstance.removeIndex(name: indexName)
        }
        try putOnThread(task: task)

    }

    /**
     Obtain a list of all existing indexes
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Index error (402)

     :returns:  A tuple containing an Array of all existing index names on the SQLite database, and an Int with the error code or nil if there was no error
     */
    public static func existingIndexes() throws -> [String] {

        var result = [String] ()
        var error: Int? = nil
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}
            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
            }
            result = try SQLiteDB.sharedInstance.existingIndexes()
        }
        try putOnThread(task: task)
        return result

    }

    /**
     Obtain a list of all existing indexes on a specific table
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Index error (402)

     :param:  table  The name of the table that is being queried for indexes

     :returns:       A tuple containing an Array of all existing index names in the table, and an Int with the error code or nil if there was no error
     */
    public static func existingIndexesForTable(table: String) throws -> [String] {

        var result = [String] ()
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}

            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
            }
            result = try SQLiteDB.sharedInstance.existingIndexesForTable(table: table)
        }
        try putOnThread(task: task)
        return result

    }


    // MARK: - Transactions and Savepoints

    /**
     Execute commands within a single exclusive transaction

     A connection to the database is opened and is not closed until the end of the transaction. A transaction cannot be embedded into another transaction or savepoint.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Transaction errors (501 - 502)

     :param: transactionClosure  A closure containing commands that will execute as part of a single transaction. If the transactionClosure returns true, the changes made within the closure will be committed. If false, the changes will be rolled back and will not be saved.

     :returns:                   An Int with the error code, or nil if there was no error committing or rolling back the transaction
     */
    public static func transaction(transactionClosure: ()->Bool) throws -> Void {

        var error: Int? = nil
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}

            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
            }
			try SQLiteDB.sharedInstance.beginTransaction()
			if transactionClosure() {
				do {
					try SQLiteDB.sharedInstance.commitTransaction()
				} catch {
					try SQLiteDB.sharedInstance.rollbackTransaction()
				}
			}
//            if let err = SQLiteDB.sharedInstance.beginTransaction() {
//                SQLiteDB.sharedInstance.close()
//                error = err
//                return
//            }
//            if transactionClosure() {
//                if let err = SQLiteDB.sharedInstance.commitTransaction() {
//                    error = err
//                }
//            } else {
//                if let err = SQLiteDB.sharedInstance.rollbackTransaction() {
//                    error = err
//                }
//            }
//            SQLiteDB.sharedInstance.close()
        }
        try putOnThread(task: task)
//        return error

    }

    /**
     Execute commands within a single savepoint

     A connection to the database is opened and is not closed until the end of the savepoint (or the end of the last savepoint, if embedded).

     NOTE: Unlike transactions, savepoints may be embedded into other savepoints or transactions.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param: savepointClosure  A closure containing commands that will execute as part of a single savepoint. If the savepointClosure returns true, the changes made within the closure will be released. If false, the changes will be rolled back and will not be saved.

     :returns:                 An Int with the error code, or nil if there was no error releasing or rolling back the savepoint
     */
    public static func savepoint(savepointClosure: ()->Bool) throws -> Void {

        var error: Int? = nil
        let task: () throws -> Void = {
			defer {
				SQLiteDB.sharedInstance.close()
			}

            if let err = SQLiteDB.sharedInstance.open() {
				throw SD.SDError.SQLITE3(code: err)
//                error = err
//                return
            }
			try SQLiteDB.sharedInstance.beginSavepoint()
			if savepointClosure() {
				try SQLiteDB.sharedInstance.releaseSavepoint()
			} else {
				do {
					try SQLiteDB.sharedInstance.rollbackTransaction()
				} catch {
					SQLiteDB.sharedInstance.savepointsOpen -= 1
				}
				try SQLiteDB.sharedInstance.releaseSavepoint()
			}
//            if let err = SQLiteDB.sharedInstance.beginSavepoint() {
//                SQLiteDB.sharedInstance.close()
//                error = err
//                return
//            }
//            if savepointClosure() {
//                if let err = SQLiteDB.sharedInstance.releaseSavepoint() {
//                    error = err
//                }
//            } else {
//                if let err = SQLiteDB.sharedInstance.rollbackSavepoint() {
//                    print("Error rolling back to savepoint")
//                    SQLiteDB.sharedInstance.savepointsOpen -= 1
//                    SQLiteDB.sharedInstance.close()
//                    error = err
//                    return
//                }
//                if let err = SQLiteDB.sharedInstance.releaseSavepoint() {
//                    error = err
//                }
//            }
//            SQLiteDB.sharedInstance.close()
        }
		try SD.putOnThread(task: task)
//        return error

    }

    /**
     Convenience function to save a UIImage to disk and return the ID
     :param: image  The UIImage to be saved
     :returns:      The ID of the saved image as a String, or nil if there was an error saving the image to disk
     */
    public static func saveUIImage(image: UIImage) -> String? {

        let docsPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0] as String
        let imageDirPath = docsPath.appending("/SwiftDataImages")
        if !FileManager.default.fileExists(atPath: imageDirPath) {
            do {
                try FileManager.default.createDirectory(atPath: imageDirPath, withIntermediateDirectories: false, attributes: nil)
            } catch {
                print("Error creating SwiftData image folder")
                return nil
            }
        }
        let imageID = NSUUID().uuidString
        let imagePath = imageDirPath.appending("/\(imageID)")
        let imageAsData = image.pngData() //NOTE: original: UIImagePNGRepresentation(image)
        let imagePathUrl = URL(fileURLWithPath: imagePath)
        do {
            try imageAsData?.write(to: imagePathUrl, options: .atomic)
        } catch {
            print("Error saving image \(error)")
            return nil
        }
        return imageID

    }

    /**
     Convenience function to delete a UIImage with the specified ID

     :param: id  The id of the UIImage

     :returns:   True if the image was successfully deleted, or false if there was an error during the deletion
     */
    public static func deleteUIImageWithID(id: String) -> Bool {

        let docsPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0] as String
        let imageDirPath = docsPath.appending("/SwiftDataImages")
        let fullPath = imageDirPath.appending("/\(id)")// imageDirPath.stringByAppendingPathComponent(id)

        do {
            try FileManager.default.removeItem(atPath: fullPath)
        } catch {
            print("remove item at \(fullPath) failed, error: \(error)")
            return false
        }
        //return NSFileManager.defaultManager().removeItemAtPath(fullPath, error: nil)
        return true
    }


    // MARK: - SQLiteDB Class
    //NOTE: remove private level
    public class SQLiteDB {

        public static let sharedInstance: SQLiteDB = SQLiteDB()
        var sqliteDB: OpaquePointer? = nil
        public var dbPath = SQLiteDB.createPath()
        var inTransaction = false
        var isConnected = false
        var openWithFlags = false
        var savepointsOpen = 0
        //let queue = dispatch_queue_create("SwiftData.DatabaseQueue", DISPATCH_QUEUE_SERIAL)
        let queue = DispatchQueue(label: "SwiftData.DatabaseQueue")

		public init() {
			
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
				print("SwiftData Error -> During: Opening Database")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
				if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
					let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
					print("                -> Details: \(errMsg)")
				}
				throw SDError.SQLITE3(code: Int(status))
			}
			isConnected = true
			return
		}

        //open a connection to the sqlite3 database
        func open() -> Int? {

            if inTransaction || openWithFlags || savepointsOpen > 0 {
                return nil
            }
            if sqliteDB != nil || isConnected {
                return nil
            }
            let status = sqlite3_open(dbPath.cString(using: .utf8)!, &sqliteDB)
            if status != SQLITE_OK {
                print("SwiftData Error -> During: Opening Database")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
                return Int(status)
            }
            isConnected = true
            return nil

        }

        //open a connection to the sqlite3 database with flags
        func openWithFlags(flags: Int32) -> Int? {

            if inTransaction {
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 302 - Cannot open a custom connection inside a transaction")
                return 302
            }
            if openWithFlags {
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 301 - A custom connection is already open")
                return 301
            }
            if savepointsOpen > 0 {
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 303 - Cannot open a custom connection inside a savepoint")
                return 303
            }
            if isConnected {
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: 301 - A custom connection is already open")
                return 301
            }
            let status = sqlite3_open_v2(dbPath.cString(using: .utf8)!, &sqliteDB, flags, nil)
            if status != SQLITE_OK {
                print("SwiftData Error -> During: Opening Database with Flags")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
                return Int(status)
            }
            isConnected = true
            openWithFlags = true
            return nil

        }

        //close the connection to to the sqlite3 database
        func close() {

            if inTransaction || openWithFlags || savepointsOpen > 0 {
                return
            }
            if sqliteDB == nil || !isConnected {
                return
            }
            let status = sqlite3_close(sqliteDB)
            if status != SQLITE_OK {
                print("SwiftData Error -> During: Closing Database")
                print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
            }
            sqliteDB = nil
            isConnected = false

        }

        //close a custom connection to the sqlite3 database
        func closeCustomConnection() throws -> Void {

            if inTransaction {
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 305 - Cannot close a custom connection inside a transaction")
				throw SD.SDError.SQLITE3(code: 305)
            }
            if savepointsOpen > 0 {
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 306 - Cannot close a custom connection inside a savepoint")
				throw SD.SDError.SQLITE3(code: 306)
            }
            if !openWithFlags {
                print("SwiftData Error -> During: Closing Database with Flags")
                print("                -> Code: 304 - A custom connection is not currently open")
				throw SD.SDError.SQLITE3(code: 304)
            }
            let status = sqlite3_close(sqliteDB)
            sqliteDB = nil
            isConnected = false
            openWithFlags = false
            if status != SQLITE_OK {
                print("SwiftData Error -> During: Closing Database with Flags")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
				throw SD.SDError.SQLITE(code: status)
            }

        }

        //create the database path
        class func createPath() -> String {

            let docsPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0] as String
            let databaseStr = "SwiftData.sqlite"
            let dbPath = docsPath.appending("/\(databaseStr)")
            return dbPath

        }

        //begin a transaction
        func beginTransaction() throws -> Void {

            if savepointsOpen > 0 {
                print("SwiftData Error -> During: Beginning Transaction")
                print("                -> Code: 501 - Cannot begin a transaction within a savepoint")
                //return 501
				throw SD.SDError.SQLITE(code: 501)
            }
            if inTransaction {
                print("SwiftData Error -> During: Beginning Transaction")
                print("                -> Code: 502 - Cannot begin a transaction within another transaction")
                //return 502
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
        func rollbackTransaction() throws -> Void {

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
        func commitTransaction() throws -> Void {

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
        func beginSavepoint() throws -> Void {

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
        func rollbackSavepoint() throws -> Void {
            //return executeChange(sqlStr: "ROLLBACK TO 'savepoint\(savepointsOpen)'")
			do {
				try executeChange(sqlStr: "ROLLBACK TO 'savepoint\(savepointsOpen)'")
			} catch {
				throw error
			}
        }

        //release a savepoint
        func releaseSavepoint() throws -> Void {

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
        func lastInsertedRowID() -> Int {
            let id = sqlite3_last_insert_rowid(sqliteDB)
            return Int(id)
        }

        //number of rows changed by last update
        func numberOfRowsModified() -> Int {
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
        func executeChange(sqlStr: String, withArgs: [AnyObject]? = nil) throws -> Void {

            var sql = sqlStr
            if let args = withArgs {
				do {
					sql = try bind(objects: args, toSQL: sql)
				} catch {
					throw error
				}
            }
            var pStmt: OpaquePointer? = nil
            var status = sqlite3_prepare_v2(SQLiteDB.sharedInstance.sqliteDB, sql, -1, &pStmt, nil)
            if status != SQLITE_OK {
                print("SwiftData Error -> During: SQL Prepare")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let sqliteDB = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(sqliteDB))
                    print("                -> Details: \(errMsg)")
                }
                sqlite3_finalize(pStmt)
				throw SD.SDError.SQLITE3(code: Int(status))
            }
            status = sqlite3_step(pStmt)
            if status != SQLITE_DONE && status != SQLITE_OK {
                print("SwiftData Error -> During: SQL Step")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let db = sqlite3_errmsg(SQLiteDB.sharedInstance.sqliteDB) {
                    let errMsg = String(cString: db)
                    print("                -> Details: \(errMsg)")
                }
                sqlite3_finalize(pStmt)
				throw SD.SDError.SQLITE3(code: Int(status))
            }
            sqlite3_finalize(pStmt)
        }

        //execute a SQLite query from a SQL String
        func executeQuery(sqlStr: String, withArgs: [AnyObject]? = nil) throws -> [SDRow] {

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
            var status = sqlite3_prepare_v2(SQLiteDB.sharedInstance.sqliteDB, sql, -1, &pStmt, nil)
            if status != SQLITE_OK {
				let error = SDError.SQLITE(code: status)
                print("SwiftData Error -> During: SQL Prepare")
				print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                if let db = SQLiteDB.sharedInstance.sqliteDB {
                    let errMsg = String(cString: sqlite3_errmsg(db))
                    print("                -> Details: \(errMsg)")
                }
                sqlite3_finalize(pStmt)
                //return (resultSet, Int(status))
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
					let error = SDError.SQLITE(code: status)
                    print("SwiftData Error -> During: SQL Step")
					print("                -> Code: \(status) - " + SDError.message(code: Int(status)))
                    if let db = sqlite3_errmsg(SQLiteDB.sharedInstance.sqliteDB) {
                        let errMsg = String(cString: db)
                        print("                -> Details: \(errMsg)")
                    }
                    sqlite3_finalize(pStmt)
					throw error
                    //return (resultSet, Int(status))
                }
            }
            sqlite3_finalize(pStmt)
            //return (resultSet, nil)
			return resultSet
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
                    print("SwiftData Error -> Invalid image ID provided")
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

    private static func putOnThread(task: () throws ->Void) throws {
        if SQLiteDB.sharedInstance.inTransaction || SQLiteDB.sharedInstance.savepointsOpen > 0 || SQLiteDB.sharedInstance.openWithFlags {
            try task()
        } else {
            try SQLiteDB.sharedInstance.queue.sync() {
                try task()
            }
        }
    }

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
					let error = SD.SDError.SQLITE3(code: 201)
					print("SwiftData Error -> During: Object Binding")
					print("                -> Code: 201 - \(error.message())")
					throw error
				}
				var obj = ""
				if i {
					if let str = objects[bindIndex] as? String {
						obj = escapeIdentifier(obj: str)
					} else {
						let error = SD.SDError.SQLITE3(code: 203)
						print("SwiftData Error -> During: Object Binding")
						print("                -> Code: 203 - \(error.message()) at array location: \(bindIndex)")
						throw error
					}
					//WARN: newSql.endIndex.predecessor() -> newSql.endIndex 可能会报错
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
			let error = SD.SDError.SQLITE3(code: 202)
			print("SwiftData Error -> During: Object Binding")
			print("                -> Code: 202 - \(error.message())")
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
        fileprivate func toSQL() -> Int32 {

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


public extension SwiftData.SQLiteDB {

	typealias ColumnTypes = [String: SwiftData.DataType]

    //create a table
    func createSQLTable(table: String, withColumnsAndTypes values: [String: SwiftData.DataType]) throws -> Void {

        var sqlStr = "CREATE TABLE \(table) (ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        var firstRun = true
        for value in values {
            if firstRun {
                sqlStr += "\(escapeIdentifier(obj: value.0)) \(value.1.toSQL())"
                firstRun = false
            } else {
                sqlStr += ", \(escapeIdentifier(obj: value.0)) \(value.1.toSQL())"
            }
        }
        sqlStr += ")"
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
                print("SwiftData Error -> During: Finding Existing Tables")
                print("                -> Code: 403 - Error extracting table names from sqlite_master")
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
