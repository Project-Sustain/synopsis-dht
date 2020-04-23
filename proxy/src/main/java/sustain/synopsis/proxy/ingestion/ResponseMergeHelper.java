package sustain.synopsis.proxy.ingestion;


import com.google.protobuf.Message;

public interface ResponseMergeHelper<T extends Message> {
    T getEmptyMessage();

    T merge(T base, T newResponse);
}
