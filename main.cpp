#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <queue>
#include<random>
#include <iomanip>
#include <climits>      //to use UINT_MAX
#include <unordered_map>

using namespace std;


//PARAMETERS


#define     D_VALUE     2       //d - degree of the B-tree
#define     MIN_KEYS    (D_VALUE)
#define     MAX_KEYS    (2 * D_VALUE)

#define     DATA_PAGE_SIZE      (MAX_KEYS)      //how many records can be put in single data page (maybe change to number of bytes?)
#define INDEX_BUFFER_LIMIT      5           //how many pages can be put in buffer in RAM
#define DATA_BUFFER_LIMIT      5

#define		MANUAL_TXT_FILENAME	"./tests/manual_data.txt"
#define     RANDOM_TXT_FILENAME     "./tests/random_data.txt"

#define     DATA_DAT_FILENAME     "data.dat"
#define     INDEX_DAT_FILENAME      "index.dat"     //same for both manual and random


//COUNTERS


unsigned int read_count_data = 0;
unsigned int write_count_data = 0;
unsigned int read_count_index = 0;
unsigned int write_count_index = 0;
unsigned int next_page_id = 0;


//FORWARD DECLARATIONS

struct Data_page;
struct B_tree_page;

Data_page* get_data_page(unsigned int page_id, const string& filename);
void write_data_page(unsigned int page_id, const Data_page& page, string& filename);

B_tree_page* get_index_page(unsigned int page_id, const string& filename);
void write_index_page(unsigned int page_id, const B_tree_page& page, const string& filename);

B_tree_page* init_B_tree_page(B_tree_page *page);

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
        cout<<"Node "<<depth<<"."<<current_num<<":"<<endl;
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
            }
        }
    }
    //returns: UINT_MAX if not possible, id in parent.children_id array if possible
    unsigned int compensation_possible()
    {
        if(is_root())
        {
            cout<<"no way ze wchodzi tutaj"<<endl;
            return UINT_MAX;      //compensation impossible for the root
        }
        B_tree_page* parent = get_index_page(parent_id, INDEX_DAT_FILENAME);
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
            if (sibling->has_free_slots())
            {
                return 1;
            }
            return UINT_MAX;
        }
        else if (i == parent->keys_num)      //page is the last child
        {
            //check only left sibling
            B_tree_page* sibling = get_index_page(parent->children_id[i-1], INDEX_DAT_FILENAME);
            if (sibling->has_free_slots())
            {
                return i-1;
            }
            return UINT_MAX;
        }
        B_tree_page* sibling = get_index_page(parent->children_id[i-1], INDEX_DAT_FILENAME);      //left
        if (sibling->has_free_slots())
        {   
            return i-1;     //return left sibling
        }
        else
        {
            sibling = get_index_page(parent->children_id[i+1], INDEX_DAT_FILENAME);      //right
            if(sibling->has_free_slots())
            {
                return i+1;     //return right sibling
            }
            return UINT_MAX;     //both siblings full
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
            //left
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
                children_id[med] = all_children[med];
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
        /*write_index_page(id, *this, INDEX_DAT_FILENAME);
        write_index_page(sibling.id, sibling, INDEX_DAT_FILENAME);
        write_index_page(parent_id, parent, INDEX_DAT_FILENAME);*/
        dirty = true;
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
        B_tree_page* new_page = init_B_tree_page(&new_page_s);        //it's going to be page on the right side
        unsigned int med = keys_num / 2;
        for(int i = 0; i<keys_num-med-1;i++)
        {
            new_page->keys[i] = keys[med+1+i];        //moving keys to the new page
            new_page->children_id[i] = children_id[med+1+i];
            keys[med+1+i] = {UINT_MAX, UINT_MAX, UINT_MAX};       //clearing spot
            children_id[med+1+i] = UINT_MAX;
        }
        new_page->keys_num = keys_num - med - 1;
        new_page->children_id[new_page->keys_num] =children_id[keys_num];       //last child covered 
        children_id[keys_num] = UINT_MAX;

        //moving key to the parent and connecting the parent with the new page
        B_tree_page parent_s;
        B_tree_page* parent;
        int i = 0;
        if(!is_root())
        {
            parent = get_index_page(parent_id, INDEX_DAT_FILENAME);
            while(parent->children_id[i] != id)
            {
                cout<<"chej"<<endl;
                i++;
            }
            cout<<"wyszlo?"<<endl;
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
            keys[med] = {UINT_MAX, UINT_MAX, UINT_MAX};
            keys_num = med;

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
            parent = init_B_tree_page(&parent_s);
            parent->keys[0] = keys[med];
            keys[med] = {UINT_MAX, UINT_MAX, UINT_MAX};
            keys_num = med;
            parent_id = parent->id;
            new_page->parent_id = parent->id;
            cout<<"New page o id "<<new_page->id<<" rodzica o id: "<<new_page->parent_id<<endl;
            parent->children_id[0] = id;
            parent->children_id[1] = new_page->id;
            parent->keys_num = 1;
        }
        write_index_page(parent->id, *parent, INDEX_DAT_FILENAME);
        write_index_page(new_page->id, *new_page, INDEX_DAT_FILENAME);

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
                split();
            }
        }

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
            B_tree_page* root_page = get_index_page(root, index_dat_filename);
            root_page->print(1, 1);
        }
        else
        {
            cout<<"B-tree is empty"<<endl;
        }
    }

    B_tree_record search_for(unsigned int key)
    {
        if (is_empty())
        {
            return {UINT_MAX, UINT_MAX, UINT_MAX};      //not found
        }
        unsigned int current_page_id = root;

        ifstream index(index_dat_filename, ios::binary);
        if(!index.is_open())
        {
            cerr<<"Error: Couldn't open "<<index_dat_filename<<" file."<<endl;
            return {UINT_MAX, UINT_MAX, UINT_MAX};
        }

        B_tree_page* current_page;

        while(true)
        {
            /*index.seekg(current_page_id*sizeof(B_tree_page), ios::beg);     //jump to wanted page
            index.read((char*)(&current_page), sizeof(B_tree_page));        //current page put in variable current_page
            if (!index)
            {
                cerr<<"Error: Couldn't load page from "<<index_dat_filename<<" file."<<endl;
                return {UINT_MAX, UINT_MAX, UINT_MAX};
            }
            //read_count_index++;*/

            current_page = get_index_page(current_page_id, index_dat_filename);

            int result = current_page->bisection_search(key);
            if(result != -1)            //found on current page
            {
                return current_page->keys[result];
            }
            if(current_page->is_leaf())
            {
                return {UINT_MAX, current_page_id, UINT_MAX};      //not found
            }

            if(key < current_page->keys[0].key)
            {
                current_page_id = current_page->children_id[0];
                continue;
            }
            else if (key > current_page->keys[current_page->keys_num-1].key)
            {
                current_page_id = current_page->children_id[current_page->keys_num];
                continue;
            }
            for(int i = 1; i<current_page->keys_num; i++)
            {
                if(key<current_page->keys[i].key)
                {
                    current_page_id = current_page->children_id[i];
                    break;
                }
            }
        }

        index.close();
    }

    void insert(B_tree_record new_rec)
    {
        B_tree_record rec = search_for(new_rec.key);
        if (rec.key != UINT_MAX)
        {
            cout<<"Record with key "<<new_rec.key<<" already exists in the B-tree."<<endl;
            return;
        }
        unsigned int current_page_id = rec.page_id;
        B_tree_page* current_page = get_index_page(current_page_id, index_dat_filename);

        cout<<"Strona, do ktorej wstawiamy przed:";
        current_page->print(1, 1);
        cout<<endl;

        cout<<"Strona o id "<<current_page_id<<"ma rodzica o id "<<current_page->parent_id<<endl;
        current_page->insert(new_rec);

        B_tree_page* root_p = get_index_page(root, index_dat_filename);
        if(!root_p->is_root())
        {
            root = root_p->parent_id;
        }

        cout<<"Strona, do ktorej wstawiamy po:";
        current_page->print(1, 1);
        cout<<endl;
    }
};


//PAGE BUFFERS


unordered_map<unsigned int, B_tree_page> index_buffer;
unordered_map<unsigned int, Data_page> data_buffer;


//FUNCTIONS


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
    unsigned int current_page_id = 0;
    Record r;
    while (true) {
        current_page.rec_num = 0;
        current_page.id = current_page_id;
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
        current_page_id++;
    }
    in.close();
    out.close();
}

B_tree_page* init_B_tree_page(B_tree_page* page)
{
    cout<<"Init wywolany "<<next_page_id<<". raz!"<<endl;
    page->id = next_page_id;
    page->keys_num = 0;
    page->parent_id = UINT_MAX;
    page->dirty = true;

    for (unsigned int i = 0; i < MAX_KEYS + 1; i++)
    {
        page->keys[i] = {UINT_MAX, page->id, UINT_MAX};
    }

    for (unsigned int i = 0; i < MAX_KEYS + 2; i++)
    {
        page->children_id[i] = UINT_MAX;
    }

    next_page_id++;

    return page;
}


B_tree_page* get_index_page(unsigned int page_id, const string& filename)
{
    if (page_id == UINT_MAX)
    {
        cout<<"Tried to get page number UINT_MAX"<<endl;
        return nullptr;
    }

    //page in RAM - don't read it from disk
    unordered_map<unsigned int, B_tree_page>::iterator it = index_buffer.find(page_id);
    if (it != index_buffer.end())
        return &it->second;

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
    /*if (index_buffer.size() >= INDEX_BUFFER_LIMIT)
    {
        unordered_map<unsigned int, B_tree_page>::iterator it = index_buffer.find(page_id);     //removing first page from the buffer

        // Jeśli była modyfikowana → zapis
        // (u Ciebie możesz dodać bool dirty do B_tree_page tak jak w Data_page)
        // write_index_page(it->first, it->second, INDEX_DAT_FILENAME);

        if (it != index_buffer.end())
        {
            return it->second;
        }
    }*/

    index_buffer[page_id] = page;

    return &index_buffer[page_id];
}

void write_index_page(unsigned int page_id, const B_tree_page& page, const string& filename)
{
    fstream index(filename, ios::binary | ios::in | ios::out);
    if (!index.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return;
    }

    index.seekp(page_id * sizeof(B_tree_page), ios::beg);
    index.write(reinterpret_cast<const char*>(&page), sizeof(B_tree_page));

    write_count_index++;
    index_buffer[page_id] = page;
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
    /*if (data_buffer.size() >= DATA_BUFFER_LIMIT)
    {
        unordered_map<unsigned int, Data_page>::iterator it = data_buffer.find(page_id);     //removing first page from the buffer

        // Jeśli była modyfikowana → zapis
        // (u Ciebie możesz dodać bool dirty do B_tree_page tak jak w Data_page)
        // write_index_page(it->first, it->second, INDEX_DAT_FILENAME);

        if (it != data_buffer.end())
        {
            return it->second;
        }
    }*/
    data_buffer[page_id] = page;
    return &data_buffer[page_id];
}

void write_data_page(unsigned int page_id, const Data_page& page, const string& filename)
{
    fstream data(filename, ios::binary | ios::in | ios::out);
    if (!data.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return;
    }

    data.seekp(page_id * sizeof(Data_page), ios::beg);
    data.write(reinterpret_cast<const char*>(&page), sizeof(Data_page));

    write_count_data++;
    data_buffer[page_id] = page;
}



void create_b_tree(const string& data_filename)
{
    // Otwórz pliki
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

    // 1) Wyczyść bufor stron indeksu
    index_buffer.clear();

    // 2) Zainicjalizuj puste B-drzewo
    B_tree tree;
    B_tree* tree_p = &tree;
    tree_p->root = UINT_MAX;
    tree_p->index_dat_filename = INDEX_DAT_FILENAME;
    tree_p->data_dat_filename = DATA_DAT_FILENAME;

    // 3) Wczytuj dane z data.dat — strona po stronie
    Data_page dpage;
    unsigned int dpage_id = 0;

    while (true)
    {
        data.seekg(dpage_id * sizeof(Data_page), ios::beg);
        data.read((char*)&dpage, sizeof(Data_page));

        if (!data) break; // koniec pliku

        // 4) Każdy rekord ze strony data → dodaj do B-tree
        for (unsigned int i = 0; i < dpage.rec_num; i++)
        {
            if (dpage.slot_free[i]) continue;

            Record r = dpage.records[i];

            B_tree_record rec;
            rec.key = r.key;
            rec.page_id = dpage_id;
            rec.offset = i;

            cout<<r.key<<endl;

            // 5) Wstaw do drzewa

            if (tree_p->root == UINT_MAX)
            {
                // Tworzymy stronę korzenia
                /*B_tree_page root;
                root.id = 0;
                root.keys_num = 0;
                root.parent_id = UINT_MAX;
                root.is_leaf() = true;
                root.dirty = false;
                for (int i=0; i<=MAX_KEYS+1; i++)
                {
                    root.children_id[i] = UINT_MAX;
                }

                for (int i=0; i<=MAX_KEYS; i++)
                {
                    root.keys[i] = {UINT_MAX, UINT_MAX, UINT_MAX};
                }*/

                B_tree_page root_page;
                B_tree_page* root = init_B_tree_page(&root_page);

                // zapis do pliku + bufora
                write_index_page(0, *root, INDEX_DAT_FILENAME);

                tree_p->root = 0;
            }

            //B_tree_page* root_page = get_index_page(tree.root, INDEX_DAT_FILENAME);
            tree_p->insert(rec);
            //root_page->insert(rec);
        }

        dpage_id++;
    }

    data.close();
    index.close();

    cout << "B-tree successfully created from " << data_filename << endl;

    tree_p->print();
}


void show_menu()
{

    bool program_on = true;
    while(program_on)
    {
        //opcje - generuj rekordy/wczytaj z pliku/zakoncz

        //potem dodaj/usun rekord/zakoncz
    }
}

//MAIN

int main()
{
    //show_menu();
    txt_to_dat(MANUAL_TXT_FILENAME, DATA_DAT_FILENAME);
    create_b_tree(DATA_DAT_FILENAME);
    return 0;
}