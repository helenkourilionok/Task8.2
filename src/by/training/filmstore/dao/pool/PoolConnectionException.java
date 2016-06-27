package by.training.filmstore.dao.pool;

public class PoolConnectionException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4075154611865909235L;


	public PoolConnectionException(String message) {
        super(message);
    }

    public PoolConnectionException(Exception exception) {
        super(exception);
    }


    public PoolConnectionException(String message, Exception exception) {
        super(message, exception);
    }
}
