//
//  Errors.swift
//  SwiftData
//
//  Created by Deheng Xu on 2022/1/7.
//

import Foundation

// MARK: - Error Handling

public extension SwiftData {

    enum SDError: Error {
		case SQLITE(code: Int)
    }

	static func message(error: SDError) -> String {
		if case SDError.SQLITE(let code) = error {
			return SDError.message(code: code)
		} else {
			return "Not sqlite3 error \(error)"
		}
	}

}

// MARK: - SDError Functions
extension SD.SDError {

    public func message() -> String {
        return SD.message(error: self)
    }

    public static func error<T>(code: T) -> SD.SDError where T: SignedInteger {
        return .SQLITE(code: Int(code))
    }

	//get the error message from the error code
	//NOTE: remove private level
    public static func message<T>(code: T) -> String where T: SignedInteger {
		switch Int(code) {

				//no error

			case -1:
				return "No error"

				//SQLite error codes and descriptions as per: http://www.sqlite.org/c3ref/c_abort.html
			case 0:
				return "Successful result"
			case 1:
				return "SQL error or missing database"
			case 2:
				return "Internal logic error in SQLite"
			case 3:
				return "Access permission denied"
			case 4:
				return "Callback routine requested an abort"
			case 5:
				return "The database file is locked"
			case 6:
				return "A table in the database is locked"
			case 7:
				return "A malloc() failed"
			case 8:
				return "Attempt to write a readonly database"
			case 9:
				return "Operation terminated by sqlite3_interrupt()"
			case 10:
				return "Some kind of disk I/O error occurred"
			case 11:
				return "The database disk image is malformed"
			case 12:
				return "Unknown opcode in sqlite3_file_control()"
			case 13:
				return "Insertion failed because database is full"
			case 14:
				return "Unable to open the database file"
			case 15:
				return "Database lock protocol error"
			case 16:
				return "Database is empty"
			case 17:
				return "The database schema changed"
			case 18:
				return "String or BLOB exceeds size limit"
			case 19:
				return "Abort due to constraint violation"
			case 20:
				return "Data type mismatch"
			case 21:
				return "Library used incorrectly"
			case 22:
				return "Uses OS features not supported on host"
			case 23:
				return "Authorization denied"
			case 24:
				return "Auxiliary database format error"
			case 25:
				return "2nd parameter to sqlite3_bind out of range"
			case 26:
				return "File opened that is not a database file"
			case 27:
				return "Notifications from sqlite3_log()"
			case 28:
				return "Warnings from sqlite3_log()"
			case 100:
				return "sqlite3_step() has another row ready"
			case 101:
				return "sqlite3_step() has finished executing"

				//custom SwiftData errors
				//->binding errors

			case 201:
				return "Not enough objects to bind provided"
			case 202:
				return "Too many objects to bind provided"
			case 203:
				return "Object to bind as identifier must be a String"

				//->custom connection errors
			case 301:
				return "A custom connection is already open"
			case 302:
				return "Cannot open a custom connection inside a transaction"
			case 303:
				return "Cannot open a custom connection inside a savepoint"
			case 304:
				return "A custom connection is not currently open"
			case 305:
				return "Cannot close a custom connection inside a transaction"
			case 306:
				return "Cannot close a custom connection inside a savepoint"

				//->index and table errors

			case 401:
				return "At least one column name must be provided"
			case 402:
				return "Error extracting index names from sqlite_master"
			case 403:
				return "Error extracting table names from sqlite_master"

				//->transaction and savepoint errors

			case 501:
				return "Cannot begin a transaction within a savepoint"
			case 502:
				return "Cannot begin a transaction within another transaction"

				//unknown error

			default:
				//what the fuck happened?!?
				return "Unknown error"
		}

	}

}
