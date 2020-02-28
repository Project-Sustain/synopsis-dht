package sustain.synopsis.ingestion.client.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class LRUCache<E> {

    private HashMap<E, Node> map;
    private Node listHead;
    private Node listTail;

    public LRUCache () {
        map = new HashMap<>();
        listTail = null;
    }

    public void use(E elem) {
        Node existing = map.get(elem);
        if (existing != null) {
            moveToHead(existing);

        } else {
            Node newNode = new Node(null,listHead,elem);
            map.put(elem, newNode);

            if (listHead == null) {
                listTail = newNode;
            } else {
                listHead.prev = newNode;
            }
            listHead = newNode;
        }
    }

    public E evictLRU() {
        E ret = listTail.elem;
        removeLast();
        map.remove(ret);
        return ret;
    }

    public Collection<E> evictLRU(int num) {
        Collection<E> ret = new ArrayList<>();
        while (map.size() > 0) {
            ret.add(evictLRU());
        }
        return ret;
    }

    public Collection<E> evictAll() {
        Node cur = listTail;
        listHead = null;
        listTail = null;
        map.clear();

        Collection<E> ret = new ArrayList<>(size());
        while (cur != null) {
            ret.add(cur.elem);

            Node temp = cur.prev;
            cur.prev = null;
            cur.next = null;
            cur = temp;
        }
        return ret;
    }

    public int size() {
        return map.size();
    }

    private void removeLast() {
        if (listTail == listHead) {
            listHead = null;
            listTail = null;
            return;
        }

        listTail.prev.next = null;
        listTail = listTail.prev;
    }

    private void moveToHead(Node n) {
        if (n == listHead) {
            return;
        }
        if (n == listTail) {
            n.prev.next = null;
            listTail = n.prev;
        } else {
            n.prev.next = n.next;
            n.next.prev = n.prev;
        }

        n.prev = null;
        n.next = listHead;
        listHead.prev = n;
        listHead = n;
    }

    private class Node {
        Node prev;
        Node next;
        E elem;

        public Node(Node prev, Node next, E elem) {
            this.prev = prev;
            this.next = next;
            this.elem = elem;
        }
    }

}
