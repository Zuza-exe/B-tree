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

#define     D_VALUE     2
#define     MIN_KEYS    (D_VALUE)
#define     MAX_KEYS    (2 * D_VALUE)

#define     DATA_PAGE_SIZE      (MAX_KEYS)      //how many records can be put in single data page (maybe change to number of bytes?)
#define INDEX_BUFFER_LIMIT      5           //how many pages can be put in RAM
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
    B_tree_record keys[MAX_KEYS];
    unsigned int keys_num;
    unsigned int children_id[MAX_KEYS + 1];    //id of pages
    unsigned int parent_id;
    bool is_leaf;
    bool dirty;

    bool is_root()
    {
        return parent_id == UINT_MAX;
    }
    bool is_full()
    {
        return keys_num == MAX_KEYS;
    }
    bool is_overflown()
    {
        return keys_num > MAX_KEYS;
    }
    bool is_underflown()
    {
        if (is_root())
        {
            return false;
        }
        return keys_num<MIN_KEYS;
    }
    bool has_free_slots()
    {
        return keys_num<MAX_KEYS;
    }
    //returns id of wanted key in the page, -1 if not present
    int bisection_search(unsigned int key)
    {
        int left = 0;
        int right = keys_num-1;         //DBAJ O TO BY keys[keys_num] BYŁ OSTATNIM KLUCZEM!!!!!!!

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
    void insert(B_tree_record rec)
    {
        dirty = true;
        int i = 0;
        while(rec.key > keys[i].key)
        {
            i++;
        }
        for(int j = keys_num-1; j<i; j--)
        {
            keys[j+1] = keys[j];    //moving records to the right side
            children_id[j]
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
        if (root != UINT_MAX)
        {
            return true;
        }
        return false;
    }

    B_tree_record search_for(unsigned int key)
    {
        if (is_empty())
        {
            return {UINT_MAX, UINT_MAX, UINT_MAX};      //not found
        }
        unsigned int current_page_id = root;
        B_tree_page current_page;

        ifstream index(index_dat_filename, ios::binary);
        if(!index.is_open())
        {
            cerr<<"Error: Couldn't open "<<index_dat_filename<<" file."<<endl;
            return {UINT_MAX, UINT_MAX, UINT_MAX};
        }

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

            int result = current_page.bisection_search(key);
            if(result != -1)            //found on current page
            {
                return current_page.keys[result];
            }
            if(current_page.is_leaf)
            {
                return {UINT_MAX, UINT_MAX, UINT_MAX};      //not found
            }

            if(key < current_page.keys[0].key)
            {
                current_page_id = current_page.children_id[0];
                continue;
            }
            else if (key > current_page.keys[current_page.keys_num-1].key)
            {
                current_page_id = current_page.children_id[current_page.keys_num];
                continue;
            }
            for(int i = 1; i<current_page.keys_num-1; i++)
            {
                if(key<current_page.keys[i].key)
                {
                    current_page_id = current_page.children_id[i];
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
        unsigned int current_page_id = root;
        B_tree_page current_page;

        current_page = get_index_page(current_page_id, index_dat_filename);
        if(current_page.has_free_slots())
        {
            current_page.insert(new_rec);
        }
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

B_tree_page get_index_page(unsigned int page_id, const string& filename)
{
    //page in RAM - don't read it from disk
    unordered_map<unsigned int, B_tree_page>::iterator it = index_buffer.find(page_id);
    if (it != index_buffer.end())
        return it->second;

    //page not in RAM - read it from disk
    ifstream index(filename, ios::binary);
    if (!index.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return {};
    }

    B_tree_page page;
    index.seekg(page_id * sizeof(B_tree_page), ios::beg);
    index.read(reinterpret_cast<char*>(&page), sizeof(B_tree_page));

    if (!index)
    {
        cerr << "Error: Couldn't read index page " << page_id << endl;
        return {};
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

    return page;
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

    write_count_index++;                 // ✅ zapis fizyczny
    index_buffer[page_id] = page;        // aktualizacja RAM
}

Data_page get_data_page(unsigned int page_id, const string& filename)
{
    unordered_map<unsigned int, Data_page>::iterator it = data_buffer.find(page_id);
    if (it != data_buffer.end())
        return it->second;

    ifstream data(filename, ios::binary);
    if (!data.is_open())
    {
        cerr << "Error: Couldn't open " << filename << endl;
        return {};
    }

    Data_page page;
    data.seekg(page_id * sizeof(Data_page), ios::beg);
    data.read(reinterpret_cast<char*>(&page), sizeof(Data_page));

    if (!data)
    {
        cerr << "Error: Couldn't read data page " << page_id << endl;
        return {};
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
    return page;
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
    ifstream data(data_filename, ios::binary);
    ofstream index(INDEX_DAT_FILENAME, ios::binary | ios::out | ios::trunc);

    if(!data.is_open())
    {
        cerr<<"Error: Couldn't open "<<data_filename<<" file."<<endl;
        return;
    }
    if(!index.is_open())
    {
        cerr<<"Error: Couldn't open "<<INDEX_DAT_FILENAME<<" file."<<endl;
        return;
    }

    B_tree tree;
    tree.root = UINT_MAX;   //set root as empty
    tree.index_dat_filename = INDEX_DAT_FILENAME;
    tree.data_dat_filename = DATA_DAT_FILENAME;
    unsigned int current_page_id = 0;
    Data_page current_data_page;
    B_tree_page current_b_tree_page;

    data.close();
    index.close();
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
    return 0;
}