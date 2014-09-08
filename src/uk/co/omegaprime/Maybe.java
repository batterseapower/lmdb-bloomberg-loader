package uk.co.omegaprime;

import java.util.NoSuchElementException;

class Maybe<A> {
    private final boolean isPresent;
    private final A value;

    private Maybe(boolean isPresent, A value) {
        this.isPresent = isPresent;
        this.value = value;
    }

    public static <A> Maybe<A> of(A a) {
        return new Maybe<>(true, a);
    }

    public static <A> Maybe<A> empty() {
        return new Maybe<>(false, null);
    }

    public A get() {
        if (!isPresent) throw new NoSuchElementException("No value present");
        return value;
    }

    public boolean isPresent() {
        return isPresent;
    }
}
