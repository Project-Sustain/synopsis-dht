package sustain.synopsis.storage.lsmtree;

public interface Mergeable<T> {
    void merge(T t) throws MergeError;
}
