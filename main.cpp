#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include<random>
#include <climits>      //to use UINT_MAX
#include <unordered_map>
#include <memory>       //to use unique_ptr in index buffer
#include <sstream>
#include <algorithm>    //to use find()

using namespace std;


//PARAMETERS

#define     NUMBER_OF_RECORDS       30

#define     D_VALUE     2       //d - degree of the B-tree
#define     MIN_KEYS    (D_VALUE)
#define     MAX_KEYS    (2 * D_VALUE)

#define     DATA_PAGE_SIZE      (MAX_KEYS)      //how many records can be put in single data page
#define     INDEX_BUFFER_LIMIT      10           //how many pages can be put in buffer in RAM
#define     DATA_BUFFER_LIMIT      2

#define     INSTRUCTIONS_TXT_FILENAME   "./tests/manual_instructions.txt"

#define		MANUAL_TXT_FILENAME	"./tests/70_example.txt"
#define     RANDOM_TXT_FILENAME     "./tests/random_data.txt"

#define     DATA_DAT_FILENAME     "data.dat"
#define     INDEX_DAT_FILENAME      "index.dat"     //same for both manual and random

#define     PRINT_FILES         true            //if data.dat and B-tree should be printed
#define     RANDOM_RECORDS      true


//COUNTERS


unsigned int read_count_data = 0;
unsigned int write_count_data = 0;
unsigned int read_count_index = 0;
unsigned int write_count_index = 0;
unsigned int next_data_page_id = 0;
unsigned int next_page_id = 0;      //for index pages
unsigned int free_list_head = UINT_MAX;     //to hold list of free index pages (in case they were deleted)
vector<unsigned int> data_pages_with_free_slots;


//NEEDED FORWARD DECLARATIONS


struct Record;
struct B_tree_record;
struct Data_page;
struct B_tree_page;


Data_page* get_data_page(unsigned int page_id, const string& filename);
void write_data_page(unsigned int page_id, Data_page& page, const string& filename);
void print_data_dat (const string& filename);
pair<unsigned int, unsigned int> insert_rec_in_data_dat(Record rec);
void update_rec_in_data_dat (B_tree_record rec_to_change, Record new_rec);

B_tree_page* get_index_page(unsigned int page_id, const string& filename);
void write_index_page(unsigned int page_id, B_tree_page& page, const string& filename);

void init_B_tree_page(B_tree_page *page);
void free_index_page(unsigned int id);
void remove_rec_from_data_dat(B_tree_record rec);
void flush_all_buffers(const string& data_dat_filename, const string& index_dat_filename);


//PAGE BUFFERS


unordered_map<unsigned int, unique_ptr<B_tree_page>> index_buffer;
unordered_map<unsigned int, Data_page> data_buffer;


//STRUCTS


struct Record
{
    unsigned int key;
    double sides[5];
};

struct B_tree_record
{
    unsigned int key;
    unsigned int page_id;
    unsigned int offset;
};

struct Data_page
{
    unsigned int id;
    bool dirty;     //if it was modified in RAM
    Record records[DATA_PAGE_SIZE];
    unsigned int rec_num;   //number of records used
    bool slot_free[DATA_PAGE_SIZE];     //to mark if the slot is occupied by a record

};

struct B_tree_page
{
    unsigned int id;
    B_tree_record keys[MAX_KEYS +1];    //one more in case of overflow
    unsigned int keys_num;
    unsigned int children_id[MAX_KEYS + 2];    //id of pages - one more in case of overflow
    unsigned int parent_id;
    bool dirty;
    unsigned int next_free;
    unsigned int pin_count;     //to mark currently used pages in the buffer

    bool is_leaf()
    {
        return children_id[0] == UINT_MAX;
    }

    bool is_root()
    {
        return parent_id == UINT_MAX;
    }
    bool is_full()
    {
        return keys_num == MAX_KEYS;
    }
    bool has_free_slots()
    {
        return keys_num < MAX_KEYS;
    }
    bool is_overflown()
    {
        return keys_num > MAX_KEYS;
    }
    bool is_underflown()
    {
        if (is_root())
        {
            return keys_num==0;
        }
        return keys_num<MIN_KEYS;
    }
    void print(int depth, int current_num)
    {
        for(int i = 1; i<depth; i++)
        {
            cout<<"\t";
        }
        cout<<"Node "<<depth<<"."<<current_num<<" (page id: "<<id<<"):"<<endl;
        for(int i = 1; i<depth; i++)
        {
            cout<<"\t";
        }
        for (int i = 0; i<keys_num; i++)
        {
            cout<<keys[i].key<<" ";
        }
        cout<<endl;

        if(!is_leaf())
        {
            for(int i = 0; i<keys_num+1;i++)
            {
                B_tree_page* current_child = get_index_page(children_id[i], INDEX_DAT_FILENAME); 
                current_child->print(depth+1, i+1);
                current_child->pin_count--;
            }
        }
    }
    //returns: UINT_MAX if not possible, id in parent.children_id array if possible
    unsigned int compensation_possible()
    {
        if(is_root())
        {
            return UINT_MAX;      //compensation impossible for the root
        }
        B_tree_page* parent = get_index_page(parent_id, INDEX_DAT_FILENAME);
        parent->pin_count--;

        //find position in children[] array
        int i = 0;
        while(i <= parent->keys_num && parent->children_id[i] != id)
        {
            i++;
        }

        if(i > parent->keys_num)     //error backup - shouldn't happen
        {
            cerr << "Error: child not found in parent.\n";
            return UINT_MAX;
        }
        if (i == 0)     //page is the first child
        {
            //check only right sibling
            B_tree_page* sibling = get_index_page(parent->children_id[1], INDEX_DAT_FILENAME);
            if ((is_overflown() && sibling->has_free_slots()) || (is_underflown() && sibling->keys_num > MIN_KEYS))
            {
                sibling->pin_count--;
                return 1;
            }
            return UINT_MAX;
        }
        else if (i == parent->keys_num)      //page is the last child
        {
            //check only left sibling
            B_tree_page* sibling = get_index_page(parent->children_id[i-1], INDEX_DAT_FILENAME);
            if ((is_overflown() && sibling->has_free_slots()) || (is_underflown() && sibling->keys_num > MIN_KEYS))
            {
                sibling->pin_count--;
                return i-1;
            }
            return UINT_MAX;
        }
        B_tree_page* sibling = get_index_page(parent->children_id[i-1], INDEX_DAT_FILENAME);      //left
        sibling->pin_count--;
        if ((is_overflown() && sibling->has_free_slots()) || (is_underflown() && sibling->keys_num > MIN_KEYS))
        {   
            return i-1;     //return left sibling
        }
        else
        {
            sibling = get_index_page(parent->children_id[i+1], INDEX_DAT_FILENAME);      //right
            if ((is_overflown() && sibling->has_free_slots()) || (is_underflown() && sibling->keys_num > MIN_KEYS))
            {
                sibling->pin_count--;
                return i+1;     //return right sibling
            }
            return UINT_MAX;     //both siblings full/underflown
        }
    }
    //returns id of wanted key in the page, -1 if not present
    int bisection_search(unsigned int key)
    {
        int left = 0;
        int right = keys_num-1;

        while(left<=right)
        {
            int mid = left + (right-left)/2;
            if(keys[mid].key == key)
            {
                return mid;
            }
            else if (keys[mid].key < key)
            {
                left = mid + 1;      //search in right half
            }
            else
            {
                right = mid - 1;     //search in left half
            }
        }
        return -1;      //not found
    }
    //sibling_id in the function is its id in children_id array
    void compensate(unsigned int sibling_id)
    {
        B_tree_page* parent = get_index_page(parent_id, INDEX_DAT_FILENAME);
        B_tree_page* sibling = get_index_page(parent->children_id[sibling_id], INDEX_DAT_FILENAME);
        unsigned int all_keys_num = keys_num + sibling->keys_num + 1;        //1 comes from the parent
        B_tree_record *all_keys = new B_tree_record [all_keys_num];       //temporary array to hold all the keys
        unsigned int *all_children = new unsigned int [all_keys_num+1];     //temporary array to hold children

        //checking if it's the right or left sibling and filling both temporary arrays
        bool right_sibling = false;
        if (keys[0].key < sibling->keys[0].key)
        {
            right_sibling = true;
        }
        if(right_sibling)   //right sibling
        {
            int i = 0;
            for(i = 0; i<keys_num; i++)
            {
                all_keys[i] = keys[i];
                all_children[i] = children_id[i];
            }
            all_keys[keys_num] = parent->keys[sibling_id-1];    //the parent's key
            all_children[keys_num] = children_id[keys_num];       //the last child from left sibling
            int j = 0;
            for (i = keys_num+1; i<all_keys_num; i++)
            {
                all_keys[i] = sibling->keys[j];
                all_children[i] = sibling->children_id[j];
                j++;
            }
            all_children[all_keys_num] = sibling->children_id[sibling->keys_num];     //the last child from the right sibling
        }
        else    //left sibling
        {
            int i = 0;
            for(i = 0; i<sibling->keys_num; i++)
            {
                all_keys[i] = sibling->keys[i];
                all_children[i] = sibling->children_id[i];
            }
            all_keys[sibling->keys_num] = parent->keys[sibling_id];    //the parent's key
            all_children[sibling->keys_num] = sibling->children_id[sibling->keys_num];       //the last child from the left sibling
            int j = 0;
            for (i = sibling->keys_num+1; i<all_keys_num; i++)
            {
                all_keys[i] = keys[j];
                all_children[i] = children_id[j];
                j++;
            }
            all_children[all_keys_num] = children_id[keys_num];     //the last child from the right sibling
        }
        unsigned int med = all_keys_num/2;          //median will go to the parent
        unsigned int children_med = (all_keys_num+1)/2;
        if (right_sibling)
        {
            //left
            int i = 0;
            for (i = 0; i<med; i++)
            {
                keys[i] = all_keys[i];
                if(!is_leaf())
                {
                    children_id[i] = all_children[i];
                }
            }
            if(!is_leaf())
            {
                children_id[med] = all_children[med];
            }
            keys_num = med;
            //parent
            parent->keys[sibling_id-1] = all_keys[med];
            //right
            int j = 0;
            for (i = med+1; i<all_keys_num; i++)
            {
                sibling->keys[j] = all_keys[i];
                if(!is_leaf())
                {
                    sibling->children_id[j] = all_children[i];
                }
                j++;
            }
            sibling->keys_num = all_keys_num - 1 - med;
            if(!is_leaf())
            {
                sibling->children_id[sibling->keys_num] = all_children[all_keys_num];
            }
        }
        else
        {
            //left sibling
            int i = 0;
            for (i = 0; i<med; i++)
            {
                sibling->keys[i] = all_keys[i];
                if(!is_leaf())
                {
                    sibling->children_id[i] = all_children[i];
                }
            }
            if(!is_leaf())
            {
                sibling->children_id[med] = all_children[med];
            }
            sibling->keys_num = med;
            //parent
            parent->keys[sibling_id] = all_keys[med];
            //right
            int j = 0;
            for (i = med+1; i<all_keys_num; i++)
            {
                keys[j] = all_keys[i];
                if(!is_leaf())
                {
                    children_id[j] = all_children[i];
                }
                j++;
            }
            keys_num = all_keys_num - 1 - med;
            if(!is_leaf())
            {
                children_id[sibling->keys_num] = all_children[all_keys_num];
            }
        }

        //AND setting children's parent ids
        if(!is_leaf())
        {
            for(int i = 0; i<keys_num+1; i++)
            {
                B_tree_page* current_child = get_index_page(children_id[i], INDEX_DAT_FILENAME);
                current_child->dirty = true;
                current_child->parent_id = id;
                current_child->pin_count--;       
            }
            for(int i = 0; i<sibling->keys_num+1; i++)
            {
                B_tree_page* current_child = get_index_page(sibling->children_id[i], INDEX_DAT_FILENAME);
                current_child->dirty = true;
                current_child->parent_id = sibling->id;
                current_child->pin_count--;
            }
        }
        /*write_index_page(id, *this, INDEX_DAT_FILENAME);
        write_index_page(sibling.id, sibling, INDEX_DAT_FILENAME);
        write_index_page(parent_id, parent, INDEX_DAT_FILENAME);*/
        dirty = true;
        sibling->dirty = true;
        parent->dirty = true;
        parent->pin_count--;
        sibling->pin_count--;
        delete[] all_keys;
        delete[] all_children;
    }
    void split()
    {
        if(keys_num<= MAX_KEYS)
        {
            cout<<"Error: split() function called on a wrong node!"<<endl;          //backup just in case
            return;
        }

        //filling new page
        B_tree_page new_page_s;
        B_tree_page* new_page = &new_page_s;
        init_B_tree_page(new_page);        //it's going to be page on the right side
        unsigned int med = keys_num / 2;
        new_page->dirty = true;
        new_page->pin_count++;
        for(int i = 0; i<keys_num-med-1;i++)
        {
            new_page->keys[i] = keys[med+1+i];        //moving keys to the new page
            new_page->children_id[i] = children_id[med+1+i];
            if(!is_leaf())
            {
                B_tree_page* current_child = get_index_page(new_page->children_id[i], INDEX_DAT_FILENAME);
                current_child->parent_id = new_page->id;
                current_child->dirty = true;
                current_child->pin_count--;
            }
            keys[med+1+i] = {UINT_MAX, UINT_MAX, UINT_MAX};       //clearing spot
            children_id[med+1+i] = UINT_MAX;
        }
        new_page->keys_num = keys_num - med - 1;
        new_page->children_id[new_page->keys_num] =children_id[keys_num];       //last child covered 
        new_page->pin_count--;

        if(!is_leaf())
        {
            B_tree_page* last_child = get_index_page(new_page->children_id[new_page->keys_num], INDEX_DAT_FILENAME);
            last_child->parent_id = new_page->id;
            last_child->pin_count--;
        }
        children_id[keys_num] = UINT_MAX;
        keys_num = med;

        dirty = true;
        new_page->dirty = true;

        //moving key to the parent and connecting the parent with the new page
        B_tree_page parent_s;
        B_tree_page* parent = &parent_s;
        int i = 0;
        if(!is_root())
        {
            parent = get_index_page(parent_id, INDEX_DAT_FILENAME);
            while(parent->children_id[i] != id)
            {
                i++;
            }
            //i now is index of the page in the children_id array of the parent
            for(int j = parent->keys_num; j>i; j--)
            {
                parent->keys[j] = parent->keys[j-1];
                parent->children_id[j+1] = parent->children_id[j];      //moving all keys and children to the right side
            }
            parent->children_id[i+1] = new_page->id;      //adding new page as a child
            new_page->parent_id = parent->id;
            parent->keys[i] = keys[med];
            parent->keys_num += 1;
            parent->dirty = true;
            keys[med] = {UINT_MAX, UINT_MAX, UINT_MAX};

            //overflow
            if(parent->keys_num > MAX_KEYS)
            {
                unsigned int free_sibling_id = parent->compensation_possible();
                if(free_sibling_id != UINT_MAX)
                {
                    parent->compensate(free_sibling_id);
                }
                else
                {
                    parent->split();
                }
            }
        }
        else
        {
            init_B_tree_page(parent);
            parent->keys[0] = keys[med];
            keys[med] = {UINT_MAX,  UINT_MAX, UINT_MAX};
            parent_id = parent->id;
            new_page->parent_id = parent->id;
            parent->children_id[0] = id;
            parent->children_id[1] = new_page->id;
            parent->keys_num = 1;
            parent->dirty = true;
        }
        write_index_page(parent->id, *parent, INDEX_DAT_FILENAME);
        write_index_page(new_page->id, *new_page, INDEX_DAT_FILENAME);
        parent->pin_count--;

    }
    void insert(B_tree_record rec)
    {
        dirty = true;
        int i = 0;
        while(i<keys_num && rec.key > keys[i].key)
        {
            i++;
        }
        for(int j = keys_num; j>i; j--)
        {
            keys[j] = keys[j-1];    //moving records to the right side
        }
        if(!is_leaf())
        {
            for(int j = keys_num+1; j>i+1; j--)
            {
                children_id[j] = children_id[j-1];    //moving children to the right side
            }
        }
        keys[i] = rec;          //inserting new page
        keys_num++;

        //overflow
        if(keys_num > MAX_KEYS)
        {
            unsigned int free_sibling_id = compensation_possible();
            if(free_sibling_id != UINT_MAX)
            {
                compensate(free_sibling_id);
            }
            else
            {
                pin_count++;
                split();
                pin_count--;
            }
        }
    }

    void merge(const string& filename)
    {
        B_tree_page* parent = get_index_page(parent_id, filename);

        unsigned int sibling_id;

        //find position in children[] array
        int i = 0;
        while(i <= parent->keys_num && parent->children_id[i] != id)
        {
            i++;
        }

        //deciding whether we're taking left or right sibling
        B_tree_page* sibling;
        B_tree_page* left;
        B_tree_page* right;
        unsigned int left_child_id;     //IN PARENT->CHILDREN_ID, NOT PAGE ID
        if(i<parent->keys_num)      //we take right sibling by default
        {
            sibling_id = i+1;
            sibling = get_index_page(parent->children_id[sibling_id], filename);
            left = this;
            right = sibling;
            left_child_id = sibling_id-1;
        }
        else
        {
            sibling_id = i-1;
            sibling = get_index_page(parent->children_id[sibling_id], filename);
            left = sibling;
            right = this;
            left_child_id = sibling_id;
        }

        //moving all records to the left sibling
        left->keys[left->keys_num] = parent->keys[left_child_id]; //moving the parent's record
        left->keys_num++;
        parent->keys_num--;
        for(int i = left_child_id; i<parent->keys_num; i++)
        {
            parent->keys[i] = parent->keys[i+1];
            parent->children_id[i+1] = parent->children_id[i+2];
        }
        parent->keys[keys_num] = {UINT_MAX, UINT_MAX, UINT_MAX};
        parent->children_id[keys_num+1] = UINT_MAX;

        for(int i = 0; i < right->keys_num; i++)
        {
            left->keys[left->keys_num] = right->keys[i];
            left->keys_num++;
            right->keys[i] = {UINT_MAX, UINT_MAX, UINT_MAX};
        }
        right->keys_num = 0;
        free_index_page(right->id);
        
        sibling->pin_count--;

        if(parent->is_underflown())
        {
            if(parent->is_root())
            {
                for(int i = 0; i<=MIN_KEYS; i++)
                {
                    //sign children from right to left sibling!
                    left->children_id[MIN_KEYS+i] = right->children_id[i];      //it will always be MIN_KEYS + i, otherwise merge wouldn't be called 
                }
                free_index_page(parent->id);
                parent->parent_id = 0;          //so that it won't be seen as a root anymore
                left->parent_id = UINT_MAX;     //new root
            }
            else
            {
                unsigned int sibling_id = parent->compensation_possible();
                if(sibling_id != UINT_MAX)
                {
                    parent->compensate(sibling_id);
                }
                else
                {
                    parent->merge(filename);
                }
            }
        }

        parent->pin_count--;
    }
};

struct B_tree
{
    unsigned int root;
    string index_dat_filename;
    string data_dat_filename;

    bool is_empty()
    {
        return root == UINT_MAX;
    }

    void print()
    {
        if(!is_empty())
        {
            cout<<"Printing B-tree from "<<index_dat_filename<<" file"<<endl;
            unsigned int disk_reads_before = read_count_data + read_count_index;
            unsigned int disk_writes_before = write_count_data + write_count_index;
            B_tree_page* root_page = get_index_page(root, index_dat_filename);
            root_page->print(1, 1);
            root_page->pin_count--;
            //cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
            //cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
        }
        else
        {
            cout<<"Error: Can't print B-tree: B-tree is empty"<<endl;
        }
        cout<<endl;
    }

    pair<B_tree_page*, unsigned int> search_for(unsigned int key)
    {
        if (is_empty())
        {
            return {nullptr, UINT_MAX};      //not found
        }

        unsigned int disk_reads_before = read_count_data + read_count_index;
        unsigned int disk_writes_before = write_count_data + write_count_index;

        unsigned int current_page_id = root;

        B_tree_page* current_page;

        while(true)
        {
            current_page = get_index_page(current_page_id, index_dat_filename);

            int result = current_page->bisection_search(key);
            if(result != -1)            //found on current page
            {
                current_page->pin_count--;
                return {current_page, result};
            }
            if(current_page->is_leaf())
            {
                current_page->pin_count--;
                return {current_page, UINT_MAX};      //not found
            }

            if(key < current_page->keys[0].key)
            {
                current_page->pin_count--;
                current_page_id = current_page->children_id[0];
                continue;
            }
            else if (key > current_page->keys[current_page->keys_num-1].key)
            {
                current_page->pin_count--;
                current_page_id = current_page->children_id[current_page->keys_num];
                continue;
            }
            for(int i = 1; i<current_page->keys_num; i++)
            {
                if(key<current_page->keys[i].key)
                {
                    current_page->pin_count--;
                    current_page_id = current_page->children_id[i];
                    break;
                }
            }
        }

        //shouldn't ever happen
        return {nullptr, UINT_MAX};
    }

    void insert(B_tree_record new_B_rec)
    {
        cout<<"\n\nInserting record with key "<<new_B_rec.key<<endl;
        
        unsigned int disk_reads_before = read_count_data + read_count_index;
        unsigned int disk_writes_before = write_count_data + write_count_index;

        pair<B_tree_page*, unsigned int> result = search_for(new_B_rec.key);
        B_tree_page* current_page = result.first;
        unsigned int pos = result.second;

        if (pos != UINT_MAX)
        {
            cout<<"Error: Couldn't insert record. Record with key "<<new_B_rec.key<<" already exists in the B-tree."<<endl;
            cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
            cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
            return;
        }

        current_page->pin_count++;
        current_page->insert(new_B_rec);
        current_page->pin_count--;

        B_tree_page* root_p = get_index_page(root, index_dat_filename);
        if(!root_p->is_root())
        {
            root = root_p->parent_id;
        }

        root_p->pin_count--;

        cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
        cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
        cout<<endl;

        if(PRINT_FILES)
        {
            print_data_dat(data_dat_filename);
            print();
        }
    }

    Record read_record(unsigned int key)
    {
        cout<<"\n\nReading record with key "<<key<<endl;
        
        unsigned int disk_reads_before = read_count_data + read_count_index;
        unsigned int disk_writes_before = write_count_data + write_count_index;

        pair<B_tree_page*, unsigned int> result = search_for(key);
        B_tree_page* current_page = result.first;
        unsigned int pos = result.second;

        if(pos == UINT_MAX)     //not found
        {
            cout<<"Error: Couldn't read record. Key "<<key<<" does not exist in the B-tree."<<endl;
            cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
            cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
            return {UINT_MAX, {UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX}};
        }
        Data_page* dpage = get_data_page(current_page->keys[pos].page_id, data_dat_filename);
        if (!dpage)
        {
            cerr << "Error: couldn't load data page\n";
            return {UINT_MAX, {UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX}};
        }

        if (current_page->keys[pos].offset >= DATA_PAGE_SIZE)
        {
            cerr << "Error: offset out of range\n";
            return {UINT_MAX, {UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX}};
        }

        if (dpage->slot_free[current_page->keys[pos].offset])
        {
            cerr << "Error: slot is marked as free, inconsistent state\n";
            return {UINT_MAX, {UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX}};
        }

        Record r = dpage->records[current_page->keys[pos].offset];

        cout << "Loaded record key = " << r.key << endl;
        for (int i = 0; i < 5; i++)
        {
            cout << r.sides[i] << " ";
        }
        cout << endl;

        cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
        cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
        cout<<endl;

        return {r.key, {r.sides[0], r.sides[1], r.sides[2], r.sides[3], r.sides[4]}};
    }

    void update_record(Record rec)
    {
        cout<<"\n\nUpdating record with key "<<rec.key<<" to ";
        for(int i = 0; i<5; i++)
        {
            cout<<rec.sides[i]<<" ";
        }
        cout<<endl;

        unsigned int disk_reads_before = read_count_data + read_count_index;
        unsigned int disk_writes_before = write_count_data + write_count_index;

        pair<B_tree_page*, unsigned int> result = search_for(rec.key);
        B_tree_page* page = result.first;
        unsigned int pos = result.second;
        B_tree_record rec_to_change = page->keys[pos];

        if(pos == UINT_MAX)     //not in the tree
        {
            cout<<"Error: Couldn't update record. Record with key "<<rec.key<<" does not exist in the B-tree."<<endl;
            cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
            cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
            return;
        }

        update_rec_in_data_dat(rec_to_change, rec);


        cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
        cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
        cout<<endl;

        if(PRINT_FILES)
        {
            print_data_dat(data_dat_filename);
            print();
        }
    
    }

    void remove(unsigned int key)
    {
        cout<<"\n\nRemoving record with key "<<key<<endl;
        
        unsigned int disk_reads_before = read_count_data + read_count_index;
        unsigned int disk_writes_before = write_count_data + write_count_index;

        pair<B_tree_page*, unsigned int> result = search_for(key);
        B_tree_page* page = result.first;
        unsigned int pos = result.second;
        B_tree_record rec = page->keys[pos];

        if(pos == UINT_MAX)
        {
            cout<<"Error: Couldn't remove record. Record with key "<<key<<" does not exist in the B-tree."<<endl;
            cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
            cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
            return;
        }

        //B_tree_page* page = get_index_page(rec.page_id, index_dat_filename);
        B_tree_page* page_to_check;
        remove_rec_from_data_dat(rec);

        if(!page->is_leaf())
        {
            B_tree_page* child = get_index_page(page->children_id[pos], index_dat_filename);      //left child
            bool from_left = true;
            
            while (!child->is_leaf())
            {
                child->pin_count--;
                child = get_index_page(child->children_id[child->keys_num], index_dat_filename);        //going to the right side until we reach the leaf
            }
            if(child->keys_num == MIN_KEYS)
            {
                B_tree_page* predeccesor_page = child;      //remembering predeccesor's position in case exchanging with successor also will result in merge
                //to prevent underflow nad merging, we're taking from the right side
                child = get_index_page(page->children_id[pos + 1], index_dat_filename);      //right child
                 while (!child->is_leaf())
                {
                    child->pin_count--;
                    child = get_index_page(child->children_id[0], index_dat_filename);        //going to the left side until we reach the leaf
                }
                if(child->keys_num == MIN_KEYS)
                {
                    child->pin_count--;
                    child = predeccesor_page;       //taking from the left side is prefered
                }
                else
                {
                    from_left = false;
                    predeccesor_page->pin_count--;
                }
            }
            if(from_left)
            {
                page->keys[pos] = child->keys[child->keys_num-1];        //taking the maximum value
                child->keys[child->keys_num-1] = {UINT_MAX, UINT_MAX, UINT_MAX};     //clearing spot
            }
            else
            {
                page->keys[pos] = child->keys[0];       //taking the minimum value
                for(int i = 0; i<child->keys_num-1; i++)
                {
                    child->keys[i] = child->keys[i+1];
                }
                child->keys[child->keys_num-1] = {UINT_MAX, UINT_MAX, UINT_MAX};
            }
            child->keys_num--;
            page_to_check = child;
        }
        else
        {
            for(int j = pos; j <page->keys_num-1; j++)
            {
                page->keys[j] = page->keys[j+1];
            }
            page->keys[page->keys_num - 1] = {UINT_MAX, UINT_MAX, UINT_MAX};
            page->keys_num--;
            page_to_check = page;
        }

        if(page_to_check->is_underflown())
        {
            unsigned int sibling_id = page_to_check->compensation_possible();
            if(sibling_id != UINT_MAX)
            {
                page_to_check->compensate(sibling_id);
            }
            else
            {
                page_to_check->merge(index_dat_filename);
            }
        }

        page_to_check->pin_count--;

        B_tree_page* root_p = get_index_page(root, index_dat_filename);

        if(!root_p->is_root())
        {
            root = root_p->children_id[0];
        }

        cout<<"Disk reads performed: "<<read_count_data + read_count_index - disk_reads_before<<endl;
        cout<<"Disk writes performed: "<<write_count_data + write_count_index - disk_writes_before<<endl;
        cout<<endl;

        if(PRINT_FILES)
        {
            print_data_dat(data_dat_filename);
            print();
        }
    }
};


//FUNCTIONS


void init_data_page(Data_page *dpage)
{
    dpage->rec_num = 0;
    dpage->id = next_data_page_id;
    dpage->dirty = true;
    for(int i = 0; i<DATA_PAGE_SIZE; i++)
    {
        dpage->slot_free[i] = true;
        dpage->records[i] = {dpage->id, {-1, -1, -1, -1, -1}};
    }
    data_pages_with_free_slots.push_back(dpage->id);
    next_data_page_id++;

    write_data_page(dpage->id, *dpage, DATA_DAT_FILENAME);
}

//returns id of data page and offset
pair <unsigned int, unsigned int> insert_rec_in_data_dat(Record rec)
{
    unsigned int dpage_id;
    Data_page dpage;
    Data_page* dpage_p = &dpage;
    int i = 0;
    if(!data_pages_with_free_slots.empty())
    { 
        dpage_id = data_pages_with_free_slots.front();
        dpage_p = get_data_page(dpage_id, DATA_DAT_FILENAME);
        //finding free spot
        for(i = 0; i<DATA_PAGE_SIZE; i++)
        {
            if(dpage_p->slot_free[i])
            {
                break;
            }
        }
        //checking if there are any free slots left
        bool to_delete = true;
        for (int j = i; j<DATA_PAGE_SIZE; j++)
        {
            if(dpage_p->slot_free[j])
            {
                to_delete = false;
                break;
            }
        }
        if(to_delete)        //no more free slots left
        {
            data_pages_with_free_slots.erase(data_pages_with_free_slots.begin());
        }
    }
    else
    {
        dpage_id = next_data_page_id;
        init_data_page(dpage_p);
    }
    dpage_p->records[i] = rec;
    dpage_p->slot_free[i] = false;
    dpage_p->dirty = true;
    dpage_p->rec_num++;
    return {dpage_id, i};
}

void process_operations(const string& filename, B_tree* tree)
{
    ifstream in(filename);
    if (!in.is_open())
    {
        cerr << "Error: Couldn't open operations file " << filename << endl;
        return;
    }

    string line;
    while (getline(in, line))
    {
        if (line.empty()) continue;

        // INSERT
        if (line.rfind("insert(", 0) == 0)
        {
            line = line.substr(7, line.size() - 8);

            stringstream ss(line);

            Record r;
            ss >> r.key;
            for (int i = 0; i < 5; i++)
                ss >> r.sides[i];

            pair<unsigned int, unsigned int> result = insert_rec_in_data_dat(r);
            B_tree_record to_insert;
            to_insert.key = r.key;
            to_insert.page_id = result.first;
            to_insert.offset = result.second;

            tree->insert(to_insert);
        }

        // REMOVE
        else if (line.rfind("remove(", 0) == 0)
        {
            line = line.substr(7, line.size() - 8);

            unsigned int key = stoi(line);
            tree->remove(key);
        }

        else if (line.rfind("update(", 0)==0)
        {
            line = line.substr(7, line.size() - 8);
            stringstream ss(line);
            Record r;
            ss>>r.key;
            for (int i = 0; i < 5; i++)
                ss >> r.sides[i];

            tree->update_record(r);
        }

        else if (line.rfind("read(", 0)==0)
        {
            line = line.substr(7, line.size() - 8);

            unsigned int key = stoi(line);
            tree->read_record(key);
        }

        else
        {
            cerr << "Unknown operation: " << line << endl;
        }
    }

    in.close();
}


//create data pages on disk
void txt_to_dat(const string &txt, const string &dat) {
    ifstream in(txt);
    ofstream out(dat, ios::binary | ios::out | ios::trunc);  // trunc - file will be overwritten/created

    if (!in.is_open()) {
        cerr << "Error: Couldn't open .txt file"<< endl;
        return;
    }
    if (!out.is_open()) {
        cerr << "Error: Couldn't open .dat file" << endl;
        return;
    }

    Data_page current_page;
    Record r;
    while (true) {
        current_page.rec_num = 0;
        current_page.id = next_data_page_id;
        current_page.dirty = false;
        for(int i = 0; i<DATA_PAGE_SIZE; i++)
        {           
            current_page.slot_free[i] = true;
            if (!(in >> r.key)) 
            {
                for(int k = i; k<DATA_PAGE_SIZE; k++)
                {
                    current_page.slot_free[k] = true;
                }
                data_pages_with_free_slots.push_back(current_page.id);
                break;      //eof
            }
            for (int j = 0; j < 5; j++)
            {
                in >> r.sides[j];
            }
                current_page.records[i] = r;
                current_page.rec_num++;
                current_page.slot_free[i] = false;
        }
        if (current_page.rec_num == 0)
        {
            break;
        }
        out.write((char*)&current_page, sizeof(Data_page));
        next_data_page_id++;
    }
    in.close();
    out.close();
}

void init_B_tree_page(B_tree_page* page)
{
    if(free_list_head != UINT_MAX)
    {
       page = get_index_page(free_list_head, INDEX_DAT_FILENAME);
       free_list_head = page->next_free;
    }
    else 
    {
        page->id = next_page_id;
    }
    page->keys_num = 0;
    page->parent_id = UINT_MAX;
    page->dirty = true;
    page->next_free = UINT_MAX;
    page->pin_count = 0;

    for (unsigned int i = 0; i < MAX_KEYS + 1; i++)
    {
        page->keys[i] = {UINT_MAX,  UINT_MAX, UINT_MAX};
    }

    for (unsigned int i = 0; i < MAX_KEYS + 2; i++)
    {
        page->children_id[i] = UINT_MAX;
    }

    next_page_id++;

    write_index_page(page->id, *page, INDEX_DAT_FILENAME);
}

void free_index_page(unsigned int id)
{
    B_tree_page* page = get_index_page(id, INDEX_DAT_FILENAME);
    if (!page) 
    {
        return;
    }

    page->pin_count--;
    page->next_free = free_list_head;
    free_list_head = id;

    write_index_page(id, *page, INDEX_DAT_FILENAME);
}


B_tree_page* get_index_page(unsigned int page_id, const string& filename)
{
    if (page_id == UINT_MAX)
    {
        cerr<<"Error: Tried to get index page number UINT_MAX"<<endl;
        return nullptr;
    }

    //page in RAM - don't read it from disk
    unordered_map<unsigned int, unique_ptr<B_tree_page>>::iterator it = index_buffer.find(page_id);
    if (it != index_buffer.end())
    {
        it->second->pin_count++;
        return it->second.get();
    }
    //page not in RAM - read it from disk
    ifstream index(filename, ios::binary);
    if (!index.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return nullptr;
    }

    B_tree_page page;
    index.seekg(page_id * sizeof(B_tree_page), ios::beg);
    index.read(reinterpret_cast<char*>(&page), sizeof(B_tree_page));

    if (!index)
    {
        cerr << "Error: Couldn't read index page " << page_id << endl;
        return nullptr;
    }

    read_count_index++;
    if (index_buffer.size() >= INDEX_BUFFER_LIMIT)
    {
        for (auto it = index_buffer.begin(); it != index_buffer.end(); )
        {
            if (it->second->pin_count <= 0)
            {
                if (it->second->dirty)
                {
                    write_index_page(it->first, *(it->second), filename);
                    it->second->dirty = false;
                }
                it = index_buffer.erase(it);
            }
            else
            {
                ++it;
            }

            if (index_buffer.size() < INDEX_BUFFER_LIMIT)
            {
                break;
            }
        }
    }

    if (index_buffer.size() >= INDEX_BUFFER_LIMIT)
    {
        cerr<<"Error: Index pages buffer is too small to handle operation (all pages are being used). Try setting higher INDEX_BUFFER_LIMIT."<<endl;
        return nullptr;
    }

    index_buffer[page_id] = std::make_unique<B_tree_page>(page);
    index_buffer[page_id]->pin_count++;
    return index_buffer[page_id].get();

}

void write_index_page(unsigned int page_id, B_tree_page& page, const string& filename)
{
    fstream index(filename, ios::binary | ios::in | ios::out);
    if (!index.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return;
    }

    page.dirty = false;
    index.seekp(page_id * sizeof(B_tree_page), ios::beg);
    index.write(reinterpret_cast<const char*>(&page), sizeof(B_tree_page));

    write_count_index++;
}

void flush_index_buffer(const string& filename)         //saves to the file if dirty=true
{
    for (auto& [page_id, page] : index_buffer)
    {
        if (page->dirty)
        {
            page->dirty = false;
            write_index_page(page_id, *page, filename);
        }
    }
}

Data_page* get_data_page(unsigned int page_id, const string& filename)
{
    unordered_map<unsigned int, Data_page>::iterator it = data_buffer.find(page_id);
    if (it != data_buffer.end())
        return &it->second;

    ifstream data(filename, ios::binary);
    if (!data.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return nullptr;
    }

    Data_page page;
    data.seekg(page_id * sizeof(Data_page), ios::beg);
    data.read(reinterpret_cast<char*>(&page), sizeof(Data_page));

    if (!data)
    {
        cerr << "Error: Couldn't read data page " << page_id << endl;
        return nullptr;
    }

    read_count_data++;
    if (data_buffer.size() >= INDEX_BUFFER_LIMIT)
    {
        auto victim = data_buffer.begin(); //removing first page from the buffer
        if (victim->second.dirty)
        {
            victim->second.dirty = false;
            write_data_page(victim->first, victim->second, filename);
        }
        data_buffer.erase(victim);
    }
    data_buffer[page_id] = page;
    return &data_buffer[page_id];
}

void write_data_page(unsigned int page_id, Data_page& page, const string& filename)
{
    fstream data(filename, ios::binary | ios::in | ios::out);
    if (!data.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return;
    }

    page.dirty = false;
    data.seekp(page_id * sizeof(Data_page), ios::beg);
    data.write(reinterpret_cast<const char*>(&page), sizeof(Data_page));

    write_count_data++;
    data_buffer[page_id] = page;
}

void print_data_dat(const string &filename)
{
    flush_all_buffers(DATA_DAT_FILENAME, INDEX_DAT_FILENAME);
    ifstream data(filename, ios::binary | ios::in);
    if(!data.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return;
    }

    cout<<"Contents of the "<<filename<<" file\n"<<endl;

    Data_page current_page;
    unsigned int current_id = 0;

    while(true)
    {
        data.seekg(current_id * sizeof(Data_page), ios::beg);
        data.read((char*)&current_page, sizeof(Data_page));

        if(!data)
        {
            break;      //eof
        }

        cout<<"Data page ["<<current_page.id<<"]:"<<endl;
        cout<<"dirty = "<<current_page.dirty<<endl;
        cout<<"rec_num = "<<current_page.rec_num<<endl;
        cout<<"Records:"<<endl;
        for(int i = 0; i<DATA_PAGE_SIZE; i++)
        {
            cout<<i<<": ";
            if(current_page.slot_free[i])
            {
                cout<<"free slot"<<endl;
            }
            else
            {
                cout<<"Key: "<<current_page.records[i].key<<" Sides:";
                for(int j = 0; j<5; j++)
                {
                    cout<<" "<<current_page.records[i].sides[j];
                }
                cout<<endl;
            }
        }
        cout<<endl;

        current_id++;
    }

    data.close();
}

void flush_data_buffer(const string& filename)      //modifies dirty data pages
{
    for (auto& [page_id, page] : data_buffer)
    {
        if (page.dirty)
        {
            page.dirty = false;
            write_data_page(page_id, page, filename);
        }
    }
}

void remove_rec_from_data_dat(B_tree_record rec)
{
    Data_page* dpage = get_data_page(rec.page_id, DATA_DAT_FILENAME);
    dpage->slot_free[rec.offset] = true;
    dpage->dirty = true;
    dpage->rec_num--;
    auto it = find(data_pages_with_free_slots.begin(), data_pages_with_free_slots.end(), dpage->id);
    if(it == data_pages_with_free_slots.end())
    {
        data_pages_with_free_slots.push_back(dpage->id);
    }
}

void update_rec_in_data_dat (B_tree_record rec_to_change, Record new_rec)
{
    Data_page* dpage = get_data_page(rec_to_change.page_id, DATA_DAT_FILENAME);

    for (int i = 0; i < 5; i++)
    {
        if(new_rec.sides[i] <= 0)
        {
            cout<<"Error: Record has invalid value. Sides need to be a real number larger than 0."<<endl;
            return;
        }
    }

    for(int i = 0; i<5; i++)
    {
        dpage->records[rec_to_change.offset].sides[i] = new_rec.sides[i];
    }

    dpage->dirty = true;
}

void flush_all_buffers(const string& data_dat_filename, const string& index_dat_filename)
{
    flush_index_buffer(index_dat_filename);
    flush_data_buffer(data_dat_filename);
}

void create_b_tree(B_tree* tree_p, const string& data_filename)
{
    //Opening files
    ifstream data(data_filename, ios::binary);
    ofstream index(INDEX_DAT_FILENAME, ios::binary | ios::out | ios::trunc);

    if (!data.is_open())
    {
        cerr << "Error: Couldn't open " << data_filename << endl;
        return;
    }
    if (!index.is_open())
    {
        cerr << "Error: Couldn't open " << INDEX_DAT_FILENAME << endl;
        return;
    }

    index_buffer.clear();

    //Creating empty B-tree
    tree_p->root = UINT_MAX;
    tree_p->index_dat_filename = INDEX_DAT_FILENAME;
    tree_p->data_dat_filename = DATA_DAT_FILENAME;

    Data_page dpage;
    unsigned int dpage_id = 0;

    while (true)
    {
        data.seekg(dpage_id * sizeof(Data_page), ios::beg);
        data.read((char*)&dpage, sizeof(Data_page));

        if (!data)
        {
            break;
        }

        //filling index pages
        for (unsigned int i = 0; i < dpage.rec_num; i++)
        {
            if (dpage.slot_free[i]) continue;

            Record r = dpage.records[i];

            B_tree_record rec;
            rec.key = r.key;
            rec.page_id = dpage_id;
            rec.offset = i;

            if (tree_p->root == UINT_MAX)
            {

                B_tree_page root_page;
                B_tree_page* root = &root_page;
                init_B_tree_page(root);

                write_index_page(0, *root, INDEX_DAT_FILENAME);

                tree_p->root = 0;
            }

            //tree_p->insert(r);
            tree_p->insert(rec);
        }

        dpage_id++;
    }

    data.close();
    index.close();

    cout << "B-tree successfully created from " << data_filename << endl << endl;
}

void generate_random_records (const string& filename, unsigned int n,unsigned int start = 1)
{
    std::vector<unsigned int> keys;
    keys.reserve(n);

    for (unsigned int i = 0; i < n; i++)
    {
        keys.push_back(start + i);
    }
    random_device rd;
    mt19937 gen(rd());
    shuffle(keys.begin(), keys.end(), gen);

    ofstream out(filename);
    if (!out.is_open())
    {
        cerr << "Error: couldn't open"<<filename<<"\n";
        return;
    }
    for (auto k : keys)
    {
        out << k << " 1 1 1 1 1\n";
    }

}

//MAIN

int main()
{
    if(RANDOM_RECORDS)
    {
        generate_random_records(RANDOM_TXT_FILENAME, NUMBER_OF_RECORDS);
        txt_to_dat(RANDOM_TXT_FILENAME, DATA_DAT_FILENAME);
    }
    else
    {
        txt_to_dat(MANUAL_TXT_FILENAME, DATA_DAT_FILENAME);
    }
    B_tree tree;
    B_tree* tree_p = &tree;
    create_b_tree(tree_p, DATA_DAT_FILENAME);
    process_operations(INSTRUCTIONS_TXT_FILENAME, tree_p);
    flush_all_buffers(DATA_DAT_FILENAME, INDEX_DAT_FILENAME);
    cout<<"All disk read operations: "<<read_count_data+read_count_index<<endl;
    cout<<"All disk write operations: "<<write_count_data+write_count_index<<endl;
    return 0;
}