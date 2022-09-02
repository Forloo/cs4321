package p1.databaseCatalog;

public class DatabaseCatalog {
	// private field that refers to the object
	private static DatabaseCatalog singleObject;
	                                              
	private DatabaseCatalog() {
		// constructor of the SingletonExample class
	}

	public static DatabaseCatalog getInstance() {
		// write code that allows us to create only one object
		// access the object as per our need
		if (singleObject == null) {
				singleObject = new DatabaseCatalog();
		}
	
		// returns the singleton object
		return singleObject;       
	}
}