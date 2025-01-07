package u;

import java.util.concurrent.Callable;

public class OnDemandInit<T> {

	private final Callable<T> initializer;

    private volatile T initializedValue;
    
	public OnDemandInit(Callable<T> initializer) {
        this.initializer = initializer;
    }
	
    public T get() {
        T value = initializedValue;

        if (value == null) {
            synchronized (this) {
                value = initializedValue;

                if (value == null) {
                    initializedValue = initialize();
                    value = initializedValue;
                }
            }
        }

        return value;
    }

    private T initialize() {
        try {
            return initializer.call();
        } catch (Exception e) {
            throw new RuntimeException("OnDemand initialization error!", e);
        }
    }

}
