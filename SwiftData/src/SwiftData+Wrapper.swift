//
//  SwiftDataBase.swift
//  SwiftData
//
//  Created by NicholasXu on 2022/1/7.
//

import Foundation
import SQLite3

public extension String {
    func toNSString() -> NSString {
        return NSString(string: self)
    }
}

public extension SwiftData {

    static func `default`() -> SwiftData {
        return SwiftData(db: .sharedInstance)
    }

    static func instance(_ dbPath: String) -> SwiftData {
        return SwiftData(db: .init(dbPath))
    }

    //MARK: - Opening

    func open() throws -> Void {
        try db.open()
    }

    func open(_ flags: [Int32] = [SQLITE_OPEN_CREATE, SQLITE_OPEN_READWRITE]) throws -> Void {
        try db.open(flags)
    }

    func openReadOnly() throws -> Void {
        try db.open([SQLITE_OPEN_READONLY])
    }

    func openWithFlags(flags: Int32) throws -> Void {
        try db.openWithFlags(flags: flags)
    }

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
    func executeChange(sqlStr: String) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.executeChange(sqlStr: sqlStr)
        }
        try db.putOnThread(task: task)

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
    func executeChange<T>(sqlStr: String, withArgs: [T]) throws -> Void where T: AnyObject {

        let task: () throws ->Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.executeChange(sqlStr: sqlStr, withArgs: withArgs)
        }
        try db.putOnThread(task: task)

    }

    private func find<T>(_ array:[T], _ target: T) -> Int? where T: Equatable {
        for index in 0..<array.count {
            if array[index] == target {
                return index
            }
        }
        return nil
    }

    /**
     Execute multiple SQL statements (non-queries e.g. INSERT, UPDATE, DELETE, etc.)
     This function will execute each SQL statment in the provided array, in order, and return an Int with the error code, or nil if there was no error.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param: sqlArr  An array of non-query strings of SQL to be executed (INSERT, UPDATE, DELETE, etc.)

     :returns:       An Int with the error code, or nil if there was no error
     */
    func executeMultipleChanges(sqlArr: [String]) throws -> Void {

        let task: () throws -> Void = {
            try db.open()
            defer {
                db.close()
            }
            for sqlStr in sqlArr {
                do {
                    try db.executeChange(sqlStr: sqlStr)
                } catch {
                    if let index = find(sqlArr, sqlStr) {
                        print("Error occurred on array item: \(index) -> \"\(sqlStr)\"")
                    }
                    throw error
                }
                //                if let err = SQLiteDB.sharedInstance.executeChange(sqlStr: sqlStr) {
                //                    SQLiteDB.sharedInstance.close()
                //                    if let index = find(sqlArr, sqlStr) {
                //                        print("Error occurred on array item: \(index) -> \"\(sqlStr)\"")
                //                    }
                //                    error = err
                //                    return
                //                }
            }
        }
        try db.putOnThread(task: task)

    }

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
    func executeQuery(sqlStr: String) throws -> [SDRow] {

        var result = [SDRow] ()
        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            result = try db.executeQuery(sqlStr: sqlStr)
        }
        try db.putOnThread(task: task)
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
    func executeQuery(sqlStr: String, withArgs: [AnyObject]) throws -> [SDRow] {

        var result = [SDRow] ()
        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            result = try db.executeQuery(sqlStr: sqlStr, withArgs: withArgs)
        }
        try db.putOnThread(task: task)
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
    func executeWithConnection(flags: SD.Flags, closure: ()->Void) throws -> Void {

        let task: () throws -> Void = {
            try db.openWithFlags(flags: flags.toSQL())
            closure()
            try db.closeCustomConnection()
        }
        try db.putOnThread(task: task)

    }

    //MARK: - Tables

    /**
     Create A Table With The Provided Column Names and Types
     Note: The ID field is created automatically as "INTEGER PRIMARY KEY AUTOINCREMENT"
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param:  table                The table name to be created
     :param:  columnNamesAndTypes  A dictionary where the key = column name, and the value = data type

     :returns:                     An Int with the error code, or nil if there was no error
     */
    func createTable(table: String, withColumnNamesAndTypes values: [String: SwiftData.DataType]) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.createSQLTable(table: table, withColumnsAndTypes: values)
        }
        try db.putOnThread(task: task)

    }

    /**
     Delete a SQLite table by name
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param:  table  The table name to be deleted

     :returns:       An Int with the error code, or nil if there was no error
     */
    func deleteTable(table: String) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.deleteSQLTable(table: table)
        }
        try db.putOnThread(task: task)

    }

    /**
     Obtain a list of the existing SQLite table names
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Table query error (403)

     :returns:  A tuple containing an Array of all existing SQLite table names, and an Int with the error code or nil if there was no error
     */
    func existingTables() throws -> [String] {

        var result = [String] ()
        let task: () throws -> Void = {
            try db.open()
            result = try db.existingTables()
            db.close()
        }
        try db.putOnThread(task: task)
        return result

    }

    //MARK: - Misc

    /**
     Obtain the error message relating to the provided error code
     :param: code  The error code provided
     :returns:     The error message relating to the provided error code
     */
    func errorMessageForCode(code: Int) -> String {
        return SwiftData.SDError.message(code: code)
    }

    /**
     Obtain the database path

     :returns:  The path to the SwiftData database
     */
    func databasePath() -> String {
        return db.dbPath
    }

    /**
     Obtain the last inserted row id
     Note: Care should be taken when the database is being accessed from multiple threads. The value could possibly return the last inserted row ID for another operation if another thread executes after your intended operation but before this function call.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :returns:  A tuple of he ID of the last successfully inserted row's, and an Int of the error code or nil if there was no error
     */
    func lastInsertedRowID() throws -> Int {

        var result = 0
        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            result = db.lastInsertedRowID()
        }
        try db.putOnThread(task: task)
        return result

    }

    /**
     Obtain the number of rows modified by the most recently completed SQLite statement (INSERT, UPDATE, or DELETE)
     Note: Care should be taken when the database is being accessed from multiple threads. The value could possibly return the number of rows modified for another operation if another thread executes after your intended operation but before this function call.
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :returns:  A tuple of the number of rows modified by the most recently completed SQLite statement, and an Int with the error code or nil if there was no error
     */
    func numberOfRowsModified() throws -> Int {

        var result = 0
        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            result = db.numberOfRowsModified()
        }
        try db.putOnThread(task: task)
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
    func createIndex(name: String, onColumns: [String], inTable: String, isUnique: Bool = false) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.createIndex(name: name, columns: onColumns, table: inTable, unique: isUnique)
        }
        try db.putOnThread(task: task)

    }

    /**
     Remove a SQLite index by its name
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)

     :param: indexName  The name of the index to be removed

     :returns:          An Int with the error code, or nil if there was no error
     */
    func removeIndex(indexName: String) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            try db.removeIndex(name: indexName)
        }
        try db.putOnThread(task: task)

    }

    /**
     Obtain a list of all existing indexes
     Possible errors returned by this function are:
     - SQLite errors (0 - 101)
     - Index error (402)

     :returns:  A tuple containing an Array of all existing index names on the SQLite database, and an Int with the error code or nil if there was no error
     */
    func existingIndexes() throws -> [String] {

        var result = [String] ()
        let task: () throws -> Void = {
            defer {
                db.close()
            }
            try db.open()
            result = try db.existingIndexes()
        }
        try db.putOnThread(task: task)
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
    func existingIndexesForTable(table: String) throws -> [String] {

        var result = [String] ()
        let task: () throws -> Void = {
            defer {
                db.close()
            }

            try db.open()
            result = try db.existingIndexesForTable(table: table)
        }
        try db.putOnThread(task: task)
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
    func transaction(transactionClosure: () -> Bool) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }

            try db.open()
            try db.beginTransaction()
            if transactionClosure() {
                do {
                    try db.commitTransaction()
                } catch {
                    try db.rollbackTransaction()
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
        try db.putOnThread(task: task)
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
    func savepoint(savepointClosure: ()->Bool) throws -> Void {

        let task: () throws -> Void = {
            defer {
                db.close()
            }

            try db.open()
            try db.beginSavepoint()
            if savepointClosure() {
                try db.releaseSavepoint()
            } else {
                do {
                    try db.rollbackTransaction()
                } catch {
                    db.savepointsOpen -= 1
                }
                try db.releaseSavepoint()
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
        try db.putOnThread(task: task)
        //        return error

    }

    /**
     Convenience function to save a UIImage to disk and return the ID
     :param: image  The UIImage to be saved
     :returns:      The ID of the saved image as a String, or nil if there was an error saving the image to disk
     */
    static func saveUIImage(image: UIImage) -> String? {

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
    static func deleteUIImageWithID(id: String) -> Bool {

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
}
