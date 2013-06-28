#ifndef BTREE_HPP
#define BTREE_HPP

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <mutex>
#include <stdlib.h>

#include "btree/BTInnerNode.hpp"
#include "btree/BTLeafNode.hpp"
#include "btree/BTNode.hpp"
#include "btree/BTreeBase.hpp"
#include "btree/CharIterator.hpp"
#include "segment_manager/BTreeSegment.hpp"

namespace dbi {

template <typename Key, typename C, typename KeySchema>
class BTree {
private:
    typedef BTNode<Key, C> Node;
    typedef BTInnerNode<Key, C> InnerNode;
    typedef BTLeafNode<Key, C> LeafNode;
    typedef typename LeafNode::Iterator LeafIterator;
    typedef typename InnerNode::Iterator InnerIterator;

    static_assert(sizeof(InnerNode) <= kPageSize, "InnerNode is bigger than a page");
    static_assert(sizeof(LeafNode) <= kPageSize, "LeafNode is bigger than a page");

    BTreeSegment& bsm;
    Node* rootnode = nullptr;
    BufferFrame* rootframe = nullptr;
    C c;
    KeySchema schema;
    std::mutex guard;

    template <typename V = TID>
    struct KeyCompare {
        const C& c;
        KeyCompare(const C& c) : c(c) {};
        bool operator()(const Key& a, const char* b) const {
            return c.less(a, b);
        }
        bool operator()(const char* a, const Key& b) const {
            return c.less(a, b);
        }
    };
    template <typename V = PageId>
    struct KeyEqual {
        const Key& k;
        const C& c;
        KeyEqual(const Key& key, const C& c) : k(key), c(c) {}
        bool operator()(const char* a) const {
            return c.equal(k, a);
        }
    };
    

    std::pair<BufferFrame*, Node*> getNode(PageId id, bool exclusive=kExclusive) {
        assert(id != kMetaPageId);
        BufferFrame& frame = bsm.getPage(id, exclusive);
        Node* node = reinterpret_cast<Node*>(frame.data());
        if (node->nodeType == Node::typeInner) {
            LeafNode* leafnode = static_cast<LeafNode*>(static_cast<void*>(frame.data()));
            node = leafnode;
        } else {
            InnerNode* innernode = static_cast<InnerNode*>(static_cast<void*>(frame.data()));
            node = innernode;
        }
        node->pageId = id;
        return {&frame, node};
    }
    
    std::pair<BufferFrame*, Node*> newPage(uint64_t nodeType) {
        std::pair<BufferFrame&, PageId> p = bsm.newPage();
        Node* node;
        if (nodeType == Node::typeLeaf)
            node = reinterpret_cast<LeafNode*>(p.first.data());
        else if (nodeType == Node::typeInner)
            node = reinterpret_cast<InnerNode*>(p.first.data());
        else
            assert(false && "unknown nodeType");
        node->pageId = p.second;
        node->nodeType = nodeType;
        return {&p.first, node};
    }

    std::pair<BufferFrame*, LeafNode*> newLeafNode() {
        std::pair<BufferFrame*, Node*> p = newPage(Node::typeLeaf);
        return {p.first, reinterpret_cast<LeafNode*>(p.second)};
    }

    std::pair<BufferFrame*, InnerNode*> newInnerNode() {
        std::pair<BufferFrame*, Node*> p = newPage(Node::typeInner);
        return {p.first, reinterpret_cast<InnerNode*>(p.second)};
    }

    InnerNode* castInner(Node* node) {
        if (node->nodeType == Node::typeInner)
            return static_cast<InnerNode*>(node);
        else
            return nullptr;
    }
    
    LeafNode* castLeaf(Node* node) {
        if (node->nodeType == Node::typeLeaf)
            return static_cast<LeafNode*>(node);
        else
            return nullptr;
    }

    void releaseNode(BufferFrame* frame, bool isDirty=true) {
        if (frame == rootframe) {
            guard.unlock();
        } else {
            bsm.releasePage(*frame, isDirty);
        }
    }

public:
    typedef Key key_type;
    typedef C comparator;

    BTree(BTreeSegment& bsm, C c, KeySchema schema) : bsm(bsm), c(c), schema(schema), guard() {
        std::pair<BufferFrame*, Node*> p = getNode(bsm.getRootPage());    
        rootframe = p.first;
        rootnode = p.second;
    }
    
    ~BTree() {
        bsm.releasePage(*rootframe, true);
    }
    /*
    constexpr uint64_t getLeafNodeSize() {
        return LeafNode::numkeys;
    }
    constexpr uint64_t getInnerNodeSize() {
        return InnerNode::numkeys;
    }
    */
    void insertLeafValue(LeafNode* node, const Key& k, const TupleId& tid) {
        auto it = std::upper_bound(node->begin(k.size()), node->end(k.size()), k, KeyCompare<TupleId>(c));
        if (it == node->end(k.size())) { // insert value at the end
            node->put(k, tid, node->nextindex++);
        } else { // shift all elements after insert position to make room
            std::memmove(it.pointernext(), it.pointer(), node->nextindex - it.index());
            node->put(k, tid, it);
            node->nextindex++;
        }
    }
    
    bool insert(const Key& k, const TID& tid) {
        const uint64_t keysize = k.size();
        const uint64_t numleafpairs = LeafNode::datasize / (k.size() + sizeof(TupleId));
        const uint64_t numinnerpairs = InnerNode::datasize / (k.size() + sizeof(PageId));
        assert(rootnode != nullptr);
        guard.lock();
        Node* node = rootnode;
        BufferFrame* nodeframe = rootframe;
        Node* parentnode = nullptr;
        BufferFrame* parentframe = nullptr;
        InnerIterator it;
        while (true) {
            InnerNode* innernode = castInner(node);
            if (innernode != nullptr) { // Node is an inner node (no Leaf node)
                if (innernode->nextindex >= numinnerpairs) { 
                    // Possible overflow -> preemptive split
                    std::pair<BufferFrame*, InnerNode*> p = newInnerNode();
                    Key up = splitInnerNode(innernode, p.second, k.size());
                    if (parentnode == nullptr) { // Split node was root node -> new root
                        std::pair<BufferFrame*, InnerNode*> proot = newInnerNode();
                        proot.second->put(up, rootnode->pageId, 0);
                        proot.second->rightpointer = p.second->pageId;
                        proot.second->nextindex = 1;
                        rootnode = proot.second;
                        rootframe = proot.first;
                        guard.unlock();
                        parentframe = nullptr;
                    } else {
                        InnerNode* innerparent = castInner(parentnode);
                        if (it == innerparent->last(keysize)) { // insert value at the end
                            innerparent->put(up, innerparent->rightpointer, innerparent->nextindex++);
                            innerparent->rightpointer = p.second->pageId;
                        } else { // shift all elements after insert position to make room
                            PageId tmp = it.second();
                            std::memmove(it.pointernext(), it.pointer(), (innerparent->nextindex - it.index())*it.getPairSize());
                            InnerIterator ittarget {it};
                            ittarget++;
                            ittarget.second(p.second->pageId);
                            innerparent->put(up, tmp, it);
                            innerparent->nextindex++;
                        }
                    }
                    if (c.less(k, up)) { // continue in left node 
                        bsm.releasePage(*p.first, true); // release right node
                    } else { // continue in right node
                        bsm.releasePage(*nodeframe, true); // release left node 
                        nodeframe = p.first;
                        innernode = p.second;
                        node = p.second;
                    }
                }
                if (parentframe != nullptr) releaseNode(parentframe, true);
                PageId childId;
                // Find correct child
                it = std::upper_bound(innernode->begin(keysize), innernode->end(keysize), k, KeyCompare<PageId>(c));
                if (it != innernode->end(keysize)) { // Not last pointer
                    assert(it.second() != kMetaPageId);
                    childId = it.second();
                } else { // last pointer
                    assert(innernode->rightpointer != kMetaPageId);
                    childId = innernode->rightpointer;
                }
                std::pair<BufferFrame*, Node*> p = getNode(childId, true);
                parentframe = nodeframe;
                parentnode = node;
                node = p.second;
                nodeframe = p.first;
            } else { // Node is a LeafNode
                LeafNode* leaf = castLeaf(node);
                assert(leaf != nullptr);
				if (leaf->nextindex < numleafpairs) { // node is not full
				    insertLeafValue(leaf, k, tid);
                   	if (parentframe != nullptr) releaseNode(parentframe, true);
                    releaseNode(nodeframe, true);
					return true;
				}
				// Node is full -> split
				auto p = newLeafNode();
				LeafNode* leaf2 = p.second;
				uint64_t leaf1size = numleafpairs/ 2;
				uint64_t leaf2size = numleafpairs-leaf1size;
				LeafIterator itleaf = std::upper_bound(leaf->begin(keysize), leaf->end(keysize), k, KeyCompare<TID>(c)); // find insert position
				LeafIterator itleafmiddle = leaf->begin(keysize)+leaf1size; // first element to be moved to new leaf
				std::memcpy(leaf2->begin(keysize).pointer(), itleafmiddle.pointer(), leaf2size);
				leaf2->nextindex = leaf2size;
				leaf->nextindex = leaf1size;
				if (itleaf < itleafmiddle) {
				    insertLeafValue(leaf, k, tid);
				} else {
				    insertLeafValue(leaf2, k, tid);
				}
				leaf->nextpage = leaf2->pageId;
				PageId id = leaf2->pageId;
				Key up = Key::readFromMemory((*(leaf2->begin(keysize))), schema);
				bsm.releasePage(*p.first, true); // Release leaf2

                // overflow in leaf
                //std::cout << "Overflow in leaf node\n";
                if (parentnode == nullptr) { // Leaf is root node
                    std::pair<BufferFrame*, InnerNode*> proot = newInnerNode();
                    proot.second->put(up, rootnode->pageId, 0);
                    proot.second->rightpointer = id;
                    proot.second->nextindex = 1;
                    rootnode = proot.second;
                    rootframe = proot.first;
                    guard.unlock();
                    releaseNode(nodeframe, true);
                } else { // Leaf was not root node -> insert split key in parent node
                    //std::cout << "Leaf was not root\n";
                    InnerNode* innerparent = castInner(parentnode);
                    if (it == innerparent->end(keysize)) { // insert value at the end
                        innerparent->put(up, innerparent->rightpointer, innerparent->nextindex++);
                        innerparent->rightpointer = id;
                    } else { // shift all elements after insert position to make room
                        PageId tmp = it.second();
                        std::memmove(it.pointernext(), it.pointer(), (innerparent->nextindex - it.index())*it.getPairSize());
                        InnerIterator ittarget {it};
                        ittarget++;
                        ittarget.second(id);
                        innerparent->put(up, tmp, it);
                        innerparent->nextindex++;
                    }
                    releaseNode(parentframe, true);
                    releaseNode(nodeframe, true);
                }
                return true;
            }
        }
    }


    Key splitInnerNode(InnerNode* node1, InnerNode* node2, uint64_t keysize) {
        const uint64_t numinnerpairs = InnerNode::datasize / (keysize + sizeof(PageId));
        uint64_t node1size = numinnerpairs/2 + 1;
        uint64_t node2size = numinnerpairs - node1size;
        InnerIterator itmiddle = (node1->begin(keysize))+node1size; // first value to be copied to second node
        InnerIterator itlastnode1 {itmiddle}; // last element in first node after split
        --itlastnode1;
        std::memcpy(node2->begin(keysize).pointer(), itmiddle.pointer(), node2size*itmiddle.getPairSize());
        node2->nextindex = node2size;
        node1->nextindex = node1size-1;
        node2->rightpointer = node1->rightpointer;
        node1->rightpointer = itlastnode1.second();
        Key up = Key::readFromMemory(*itlastnode1, schema);
        return up;
    }


    std::pair<Key&, PageId&> insertInLeaf(LeafNode* leaf, const Key& k, const TupleId& tid) {
        const uint64_t keysize = k.size();
        const uint64_t numleafpairs = LeafNode::datasize / (k.size() + sizeof(TupleId));
        assert(leaf != nullptr);
        if (leaf->nextindex < numleafpairs) { // node is not full
            insertLeafValue(leaf, k, tid);
            Key nk;
            return {nk, kMetaPageId};
        }
        // Node is full -> split
        //std::cout << "Overflow in leaf node " << leaf->pageId << std::endl;
        auto p = newLeafNode();
        LeafNode* leaf2 = p.second;
        uint64_t leaf1size = numleafpairs/ 2;
        uint64_t leaf2size = numleafpairs-leaf1size;
        auto it = std::upper_bound(leaf->begin(keysize), leaf->end(keysize), k, KeyCompare<TID>(c)); // find insert position
        auto itmiddle = leaf->begin(keysize)+leaf1size; // first element to be moved to new leaf
        
        std::memcpy(leaf2->begin(keysize).pointer(), itmiddle.pointer(), leaf2size);
        leaf2->nextindex = leaf2size;
        leaf->nextindex = leaf1size;
        if (it < itmiddle) {
            insertLeafValue(leaf, k, tid);
        } else {
            insertLeafValue(leaf2, k, tid);
        }
        leaf->nextpage = leaf2->pageId;
        PageId id = leaf2->pageId;
        Key up = Key::readFromMemory((*(leaf2->begin(keysize))), schema);
        bsm.releasePage(*p.first, true); // Release leaf2
        return {up, id};
    }


    std::pair<BufferFrame*, LeafNode*> leafLookup(const Key& k) {
        const uint64_t keysize = k.size();
        assert(rootnode != nullptr);
        guard.lock();
        Node* node = rootnode;
        BufferFrame* nodeframe = rootframe;
        BufferFrame* parentframe = nullptr;
        
        InnerNode* castnode = castInner(rootnode);
        while (castnode != nullptr) {
            auto it = std::upper_bound(castnode->begin(keysize), castnode->end(keysize), k, KeyCompare<PageId>(c));
            PageId childId;
            if (it != castnode->end(keysize)) {
                assert(it.second() != kMetaPageId);
                childId = it.second();
            } else {
                assert(castnode->rightpointer != kMetaPageId);
                childId = castnode->rightpointer;
            }
            std::pair<BufferFrame*, Node*> p = getNode(childId);
            if (parentframe != nullptr) releaseNode(parentframe, false);
            parentframe = nodeframe;
            nodeframe = p.first;
            node = p.second;
            castnode = castInner(node);
        }
        LeafNode* leafnode = castLeaf(node);
        assert(leafnode != nullptr);
        if (parentframe != nullptr) releaseNode(parentframe, false);
        return {nodeframe, leafnode};
    }

    bool lookup(const Key& k, TupleId& tid) {
        const uint64_t keysize = k.size();
        std::pair<BufferFrame*, LeafNode*> p = leafLookup(k);
        LeafNode* leafnode = p.second;
        LeafIterator it = std::find_if(leafnode->begin(keysize), leafnode->end(keysize), KeyEqual<TID>(k, c));
        if (it != leafnode->end(keysize)) {
            tid = it.second();
            releaseNode(p.first, false);
            return true;
        } else {
            releaseNode(p.first, false);
            return false;
        }
    }


    bool erase(const Key& k) {
        const uint64_t keysize = k.size();
        std::pair<BufferFrame*, LeafNode*> p = leafLookup(k);
        LeafNode* leafnode = p.second;
        LeafIterator it = std::find_if(leafnode->begin(keysize), leafnode->end(keysize), KeyEqual<TID>(k, c));
        if (it != leafnode->end(keysize)) {
            std::memmove(it.pointer(), (it+1).pointer(), leafnode->nextindex - it.index());
            leafnode->nextindex--;
            releaseNode(p.first, true);
            // TODO: underflow handling
            return true;
        }
        releaseNode(p.first, false);
        return false;
    }

    /*
    uint64_t size() {
        assert(rootnode != nullptr);
        guard.lock();
        Node* node = rootnode;
        BufferFrame* nodeframe = rootframe;
        BufferFrame* parentframe = nullptr;
        
        InnerNode* inode = castInner(node);
        while (inode != nullptr) {
            PageId childId = inode->begin().second();
            std::pair<BufferFrame*, Node*> p = getNode(childId);
            if (parentframe != nullptr) releaseNode(parentframe, false);
            parentframe = nodeframe;
            nodeframe = p.first;
            node = p.second;
            inode = castInner(node);
        }
        if (parentframe != nullptr) releaseNode(parentframe, false);
        LeafNode* leafnode = castLeaf(node);
        assert(leafnode != nullptr);
     
        uint64_t count = leafnode->nextindex;
        while (leafnode->nextpage != kMetaPageId) {
            PageId next = leafnode->nextpage;
            std::pair<BufferFrame*, Node*> p = getNode(next);
            releaseNode(nodeframe, false);
            nodeframe = p.first;
            node = p.second;
            leafnode = castLeaf(node);
            count += leafnode->nextindex;
        }
        releaseNode(nodeframe, false);
        return count; 
    }
    */

    class Iterator {
        BTree<Key, C, KeySchema>* tree;
        LeafNode* node;
        BufferFrame* frame;
        LeafIterator it;
        const Key& last;
        bool finished = false;
        public:

        bool operator==(const Iterator& other) const {
            return (finished && other.finished) || ((finished == other.finished) && frame == other.frame && it == other.it);
        }

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

        void next() {
            if (finished) return;
            it++;
            if (it == node->end()) {
                PageId id = node->nextpage;
                tree->releaseNode(frame, false);
                if (id != kMetaPageId) {
                    std::pair<BufferFrame*, Node*> p = tree->getNode(id);
                    node = tree->castLeaf(p.second);
                    frame = p.first;
                    it = node->begin();
                } else {
                    finished = true;
                    frame = nullptr;
                    node = nullptr; 
                }
            }
            if (c.less(last, *it)) { // next value smaller than last in Range, finish
                finished = true;
            }
        }
        const Iterator operator++() {
            Iterator result = *this;
            next();
            return result;
        }

        const Iterator& operator++(int) {
            next();
            return *this;
        }


        std::pair<Key, TID> operator*() const {
            return {(*it).first, (*it).second};
        }

        bool valid () const {
            return !finished;
        }

        Iterator(BTree<Key, C, KeySchema>* pointer, BufferFrame* frame, LeafNode* node, LeafIterator it, const Key& last) : it(it), last(last) {
            tree = pointer;
            this->node = node;
            this->frame = frame;
        }

        Iterator(BTree<Key, C, KeySchema>* pointer, const Key& last) : last(last) {
            tree = pointer;
            finished = true;
            this->node = nullptr;
            this->frame = nullptr;
        }

        ~Iterator() {
            if (frame != nullptr)
                tree->releaseNode(frame, false);
        }

    };


    Iterator lookupRange(const Key& first, const Key& last) { 
        std::pair<BufferFrame*, LeafNode*> p = leafLookup(first);
        LeafNode* leafnode = p.second;
        auto it = std::find_if(leafnode->begin(), leafnode->end(), KeyEqual<TID>(first, c));
        if (it != leafnode->end())
            return *new Iterator{this, p.first, leafnode, it, last};
        else
            return *new Iterator{this, last};
    }

    Iterator* end() {
        return new Iterator{this, Key()};
    }
    /* 
    void visualizeNode(std::ofstream& out, PageId id) {
        assert(id != kMetaPageId);
        std::pair<BufferFrame*, Node*> p;
        if (id == rootnode->pageId)
            p = {rootframe, rootnode};
        else
            p = getNode(id);
        InnerNode* node = castInner(p.second);
        if (node != nullptr) {
            visualizeInnerNode(out, node, p.first);
        } else {
            LeafNode* leafnode = castLeaf(p.second);
            assert(leafnode != nullptr);
            visualizeLeafNode(out, leafnode, p.first);
        }
    }

    void visualizeLeafNode(std::ofstream& out, LeafNode* node, BufferFrame* frame) {
        auto it = node->values.begin();
        out << "node" << node->pageId << " [shape=record, label= \"<begin>("<< node->pageId << ") ";
        while (it != node->values.begin()+node->nextindex) {
            out << " | <key> " << (*it).first; //<< " | <value> " << (*it).second;
            it++;
        }
        out << "\" ];\n";
        releaseNode(frame, false);
    }

    void visualizeInnerNode(std::ofstream& out, InnerNode* node, BufferFrame* frame) {
        auto it = node->values.begin();
        std::list<PageId> pages;
        out << "node" << node->pageId << " [shape=record, label= \"<begin>("<< node->pageId << ") | ";
        while (it != node->values.begin()+node->nextindex) {
            out << "<ptr" << (*it).second << "> * | <key> " << (*it).first << " | ";
            pages.push_back((*it).second);
            it++;
        }
        out << "<ptr" << node->rightpointer << ">*\" ];\n\n";
        pages.push_back(node->rightpointer);
        releaseNode(frame, false);
        for (PageId p : pages) {
            visualizeNode(out, p);
        }
        for (PageId p : pages) {
            out << "node" << node->pageId << ":ptr" << p << " -> node" << p <<":begin;\n";
        }
    }

    void visualize(std::string filename = "bin/var/tree.dot") {
        std::ofstream out;
        out.open(filename.c_str(), std::ofstream::out);
        out << "digraph myBTree {\nnode [shape=record];\n";
        guard.lock();
        visualizeNode(out, rootnode->pageId);
        out << "\n}\n";
        out.close();
        if (system("dot -Tpng bin/var/tree.dot -o bin/var/tree.png") != 0) {
            std::cout << "Converting tree.dot to a png-File failed" << std::endl;
        }
    }
    */
};


}


#endif
