
#ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#endif
#define _FILE_OFFSET_BITS 64
#define _TIME_BITS 64

#define PY_SSIZE_T_CLEAN

#include <Python.h>

#include <array>
#include <string>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>
#include <memory>
#include <stdexcept>

#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <endian.h>

#include <fcntl.h>
#include <errno.h>

#define likely(EXPR)   __builtin_expect(EXPR, 1)
#define unlikely(EXPR) __builtin_expect(EXPR, 0)

namespace {
// for building exception messages
struct SB {
    std::ostringstream strm;
    SB() {}
    operator std::string() const { return strm.str(); }
    std::string str() const { return strm.str(); }
    template<typename T>
    SB& operator<<(const T& i) { strm<<i; return *this; }
};

/* until GCC < 13 buffering of std::fstream has terrible performance due small fixed buffer size.
 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=63746
 * unknown if GCC >= 13 fully addresses this.
 * Until the, we do our own buffering
 */
struct rawfile {
    std::vector<char> buf;
    /* When writing: valid range is [0, pos)  limit unused
     * 0  pos   buf.size()
     * |XXX|-----|
     * When reading: valid range is [pos, limit)
     * 0  pos  limit  buf.size()
     * |---|XXXX|------|
     */
    size_t pos=0, limit=0;
    int fd = -1;
    bool writing = false;

    rawfile() = default;
    rawfile(const std::string& fname, bool write)
        :rawfile(fname.c_str(), write)
    {}
    rawfile(const char *fname, bool write)
        :buf(64*1024*1024)
        ,fd(open(fname, (write ? O_CREAT|O_EXCL|O_RDWR : O_RDONLY) | O_LARGEFILE, 0444))
        ,writing(write)
    {
        if(fd==-1) {
            int err = errno;
            throw std::runtime_error(SB()<<"Failed to open '"<<fname<<"' : "<<err<<" "<<strerror(err));
        }
    }
    rawfile(const rawfile&) = delete;
    rawfile& operator=(const rawfile&) = delete;

    rawfile(rawfile&& o) noexcept {
        rawfile().swap(o);
        o.swap(*this);
    }
    rawfile& operator=(rawfile&& o) noexcept {
        rawfile().swap(o);
        o.swap(*this);
        return *this;
    }

    void swap(rawfile& o) noexcept {
        std::swap(buf, o.buf);
        std::swap(pos, o.pos);
        std::swap(limit, o.limit);
        std::swap(fd, o.fd);
        std::swap(writing, o.writing);
    }

    ~rawfile() {
        try {
            close();

        }catch(std::exception& e){
            std::cerr<<"Error on "<<__func__<<" : "<<e.what()<<"\n";
        }
    }

    bool is_open() const {
        return fd>=0;
    }

    void close() {
        if(fd<0)
            return;
        if(writing)
            flush();
        pos = limit = 0;

        while(true) {
            int ret = ::close(fd);
            if(ret==0)
                break;

            int err = errno;
            if(err != EINTR)
                throw std::runtime_error(SB()<<"Error on close : "<<err<<" "<<strerror(err));
        }
        fd = -1;
    }

    /* Ensure read buffer contains at least "need" bytes.
     * Return true if so.
     * Return false if file exactly at EoF and buffer empty
     * Throw otherwise
     */
    bool ensure(size_t need) {
        if(unlikely(writing || pos > limit))
            throw std::logic_error(SB()<<__func__<<" pre-condition violation");

        if(likely(limit-pos >= need))
            return true;

        if(pos!=limit) {
            memmove(buf.data(),
                    buf.data()+pos,
                    limit-pos);
            limit -= pos;
            pos = 0;

        } else {
            pos = limit = 0;
        }

        while(limit-pos < need) {
            auto ret = ::read(fd, buf.data()+limit, buf.size()-limit);
            if(ret<0) {
                int err = errno;
                throw std::runtime_error(SB()<<"Failed to read "<<err<<" "<<strerror(err));
            } else if(ret==0) {
                if(limit==0)
                    return false;

                throw std::runtime_error("Unexpected EoF");
            }
            limit += ret;
        }
        return true;
    }

    bool read(void *out, size_t request) {
        if(unlikely(!ensure(request)))
            return false;
        memcpy(out, buf.data()+pos, request);
        pos += request;
        return true;
    }

    inline
    void drain(size_t request) {
        if(unlikely(!ensure(request)))
            throw std::runtime_error("Unexpected EoF");
        pos += request;
    }

    template<typename T>
    inline
    bool read_into(T& out) {
        return this->read(&out, sizeof(out));
    }

    template<typename T>
    inline
    T read_as() {
        T ret;
        if(unlikely(!read_into(ret)))
            throw std::runtime_error("Unexpected EoF");
        return ret;
    }

    void flush() {
        if(unlikely(!writing))
            throw std::logic_error(SB()<<__func__<<" pre-condition violation");
        for(size_t i=0; i<pos; ) {
            auto ret = ::write(fd, buf.data()+i, pos-i);
            if(unlikely(ret<=0)) {
                int err = errno;
                throw std::runtime_error(SB()<<"Failed to read "<<err<<" "<<strerror(err));
            }
            i += ret;
        }
        pos = 0;
    }

    inline
    void write(const void *in, size_t request) {
        if(unlikely(buf.size()-pos < request))
            flush();
        // flush is always complete
        memcpy(buf.data()+pos, in, request);
        pos += request;
    }

    template<typename T>
    inline
    void write_from(const T&in) {
        this->write(&in, sizeof(in));
    }

    size_t seek(size_t off, int whence = SEEK_SET) {
        if(writing)
            flush();
        pos = limit = 0;
        auto ret = ::lseek(fd, 0, whence);
        if(unlikely(ret < 0)) {
            auto err = errno;
            throw std::runtime_error(SB()<<"Unable to lseek : "<<err<<" "<<strerror(err));
        }
        return ret;
    }

    // file position of 'pos'
    inline
    size_t tell() const {
        auto off = ::lseek(fd, 0, SEEK_CUR);
        if(unlikely(off < 0)) {
            auto err = errno;
            throw std::runtime_error(SB()<<"Unable to lseek : "<<err<<" "<<strerror(err));
        }
        return off+pos;
    }
};

struct PSCHead {
    uint16_t ps;
    uint16_t msgid;
    uint32_t msglen;
    uint32_t rxsec;
    uint32_t rxns;
};

struct QuartzNA { // payload prefix for "NA" and "NB
    uint32_t status;
    uint32_t chmask;
    uint64_t seqno;
    uint32_t sec;
    uint32_t ns;
};

struct QuartzNB {
    // extra payload prefix for "NB" only
    uint32_t hihi;
    uint32_t hi;
    uint32_t lo;
    uint32_t lolo;
};

struct priv {
    uint64_t last_seqno;
    uint64_t last_ns;
    uint32_t last_chmask;
    size_t last_nsamp;
    std::array<uint32_t, 32> last_channel;
    bool first = true;
    bool force = false;

    std::string outdir;

    std::array<rawfile, 32> out_channel;
    rawfile out_status;

    // list of corrected/non-fatel errors
    std::vector<std::string> errors;

    void prepare_output();
    void finalize_output();
};

void convert1(priv& pvt, const std::string& indat)
{
    rawfile istrm(indat.c_str(), false);

    PSCHead head;
    while(istrm.read_into(head)) {
        uint16_t msgid = be16toh(head.msgid);
        uint32_t msglen = be32toh(head.msglen);
        bool hasB = false;

        // headers already validated during recording process.
        // An error here implies some later disk error.
        // Treat as fatal
        if(be16toh(head.ps)!=0x5053 || msglen<sizeof(QuartzNA)) { // "PS"
            throw std::runtime_error(SB()<<"Corrupt header in '"<<indat<<"' near "<<istrm.tell());
        }
        if(!istrm.ensure(msglen)) {
            throw std::runtime_error(SB()<<"Truncated msg in '"<<indat<<"' near "<<istrm.tell());
        }

        switch(msgid) {
        case 0x4e41: // "NA"
            break;
        case 0x4e42: // "NB"
            hasB = true;
            if(msglen<sizeof(QuartzNA)+sizeof(QuartzNB))
                throw std::runtime_error(SB()<<"Corrupt headerB in '"<<indat<<"' near "<<istrm.tell());
            break;
        default:
            istrm.drain(msglen);
            continue;
        }

        auto hdrA(istrm.read_as<QuartzNA>());
        msglen -= sizeof(QuartzNA);
        auto chmask = be32toh(hdrA.chmask);
        auto seqno = be64toh(hdrA.seqno);
        auto nsec = uint64_t(be32toh(hdrA.sec))*1000000000 + be32toh(hdrA.ns);

        if(pvt.first) {
            pvt.first = false;
            pvt.last_chmask = chmask;
            pvt.prepare_output();

        } else {
            if(pvt.last_chmask != chmask)
                throw std::runtime_error("channel mask changes mid-stream not supported");

            auto nchan = __builtin_popcount(chmask);

            if(pvt.last_seqno+1 != seqno) {
                // eg. expect 15, have 17.  15 and 16 missing.
                auto nmissing = seqno - (pvt.last_seqno+1);
                auto deltaT = (nsec - pvt.last_ns)*1e-9;
                auto Fsamp = (nmissing*pvt.last_nsamp/nchan)/deltaT;

                pvt.errors.emplace_back(SB()
                                        <<"Missing "<<nmissing<<" ["<<(pvt.last_seqno+1)
                                        <<", "<<seqno<<") "<<deltaT<<" s"
                                        );

                if(!pvt.force && (Fsamp < 0.9e3 || Fsamp>290e3))
                    throw std::runtime_error(SB()<<"Inconsistency between timestamp "
                                             <<deltaT<<" and seqno "<<nmissing<<", Fsamp "<<Fsamp);

                const auto chmask = pvt.last_chmask;

                // inject placeholder samples based on last packet processed
                while(nmissing--) {
                    auto nsamp = pvt.last_nsamp;

                    while(nsamp) {
                        for(unsigned i=0; i<32; i++) {
                            if(!((1u<<i) & chmask))
                                continue;

                            auto s = pvt.last_channel[i];
                            pvt.out_channel[i].write_from(s);
                            nsamp--;
                        }
                    }
                }
            }
        }
        pvt.last_seqno = seqno;
        pvt.last_ns = nsec;

        if(hasB) {
            auto hdrB(istrm.read_as<QuartzNB>());
            msglen -= sizeof(QuartzNB);
            (void)hdrB;

            // TODO: emit status
        }

        auto nsamp = msglen/3u;
        pvt.last_nsamp = nsamp;

        // 'pos' pointed at first byte of first sample
        auto cur = (const uint8_t*)istrm.buf.data() + istrm.pos;

        while(nsamp) {
            // first sample in each packet is for first channel in mask.
            // each packet contains only complete time points
            for(unsigned i=0; i<32; i++) {
                if(!((1u<<i) & chmask))
                    continue;

                if(!nsamp)
                    throw std::runtime_error("Trucated body");

                auto s = uint32_t(cur[0])<<16u | uint32_t(cur[1])<<8u | uint32_t(cur[2]);
                if(s&0x00800000)
                    s |= 0xff000000; // sign extend
                cur += 3;
                nsamp--;

                pvt.last_channel[i] = s;
                pvt.out_channel[i].write_from(s);
            }
        }

        // warn on leftovers?

        istrm.drain(msglen);
    }
}

void convert2j(const std::vector<std::string>& indats,
               const std::string& outdir,
               std::vector<std::string>& errors,
               bool force)
{
    priv pvt{};
    pvt.outdir = outdir;
    pvt.force = force;

    for(auto& indat : indats) {
        convert1(pvt, indat);
    }

    pvt.finalize_output();
    errors = std::move(pvt.errors);
}

void priv::prepare_output()
{
    auto chmask = last_chmask; // in this context, the last received is the first
    if(!chmask)
        throw std::logic_error(SB()<<__func__<<" Missing chmask");

//    rawfile(SB()<<outdir<<"/STATUS.j", true)
//            .swap(out_status);

    for(unsigned i=0; i<32; i++) {
        if(!((1u<<i) & chmask))
            continue;

        auto& out = out_channel[i];

        // eg. "CH01.j"
        rawfile(SB()<<outdir<<"/CH"<<std::dec<<std::setw(2)<<std::setfill('0')<<i<<".j", true)
                .swap(out);

        // invalid placeholder
        uint32_t hdr[5] = {0xffffffff, 0xffffffff, 0xffffffff, 0, 0};
        out.write(hdr, sizeof(hdr));
    }
}

void priv::finalize_output()
{
    // TODO: finish out_status

    for(unsigned i=0; i<32; i++) {
        if(!((1u<<i) & last_chmask))
            continue;

        uint32_t hdr[5] = {1, 0, 0, 0, 0};

        auto& out = out_channel[i];
        out.flush();
        auto fsize = out.tell() - sizeof(hdr);
        out.seek(0);
        memcpy(&hdr[3], &fsize, sizeof(fsize)); // yup, size stored unaligned...
        out.write(hdr, sizeof(hdr));
        out.close();
    }
}

struct PyRef {
    PyObject *obj = nullptr;

    PyRef() = default;
    PyRef(const PyRef& o) noexcept
        :obj(o.obj)
    {
        Py_XINCREF(o.obj);
    }
    PyRef& operator=(const PyRef& o) noexcept
    {
        if(this!=&o) {
            Py_XINCREF(o.obj);
            Py_XDECREF(obj);
            obj = o.obj;
        }
        return *this;
    }

    PyRef(PyRef&& o) noexcept
        :obj(o.obj)
    {
        o.obj = nullptr;
    }
    PyRef& operator=(PyRef&& o) noexcept {
        Py_XDECREF(obj);
        obj = o.obj;
        o.obj = nullptr;
        return *this;
    }

    ~PyRef() {
        Py_CLEAR(obj);
    }

    explicit PyRef(PyObject* o)
        :obj(o)
    {
        if(!obj)
            throw std::logic_error("Alloc failed");
    }

    static
    PyRef allownull(PyObject *o) noexcept {
        PyRef ret;
        ret.obj = o;
        return ret;
    }

    static
    PyRef borrow(PyObject* o) {
        PyRef ret(o);
        Py_INCREF(o);
        return ret;
    }

    static
    PyRef iternext(const PyRef& iter) {
        auto item = PyIter_Next(iter.obj);
        auto ret(PyRef::allownull(item));
        if(!item && PyErr_Occurred())
            throw std::runtime_error("XXX"); // exception already set
        return ret;
    }

    PyObject* release() noexcept {
        auto ret = obj;
        obj = nullptr;
        return ret;
    }

    void swap(PyRef& o) noexcept {
        std::swap(obj, o.obj);
    }

    void reset(PyObject *o) {
        (*this) = PyRef(o);
    }

    void clear() noexcept {
        Py_CLEAR(obj);
    }

    struct Acquisition {
        PyRef *ref;
        PyObject *val = nullptr;
        operator PyObject**() noexcept { return &val; }
        constexpr explicit Acquisition(PyRef* ref) : ref(ref) {}
        ~Acquisition() {
            ref->reset(val);
        }
    };
    Acquisition acquire() noexcept { return Acquisition{this}; }

    explicit operator bool() const { return obj; }
};

PyObject* call_convert2j(PyObject *unused, PyObject *args, PyObject *kws) noexcept
{
    static const char* kwnames[] = {"indats", "outdir", "force", nullptr};
    try{
        (void)unused;

        PyObject *indats_py = nullptr;
        PyRef outdir_py;
        int force = false;

        if(!PyArg_ParseTupleAndKeywords(args, kws, "O!O&|p", const_cast<char**>(kwnames),
                             &PyList_Type, &indats_py,
                             PyUnicode_FSConverter, (PyObject**)outdir_py.acquire(),
                             &force))
            return NULL;

        std::vector<std::string> indats;
        for(size_t i=0, N=PyList_Size(indats_py); i<N; i++) {
            auto item = PyList_GetItem(indats_py, i);
            if(!item)
                return nullptr;
            PyRef indat_py(PyUnicode_EncodeFSDefault(item));
            indats.push_back(PyBytes_AsString(indat_py.obj));
        }

        std::vector<std::string> errors;

        Py_BEGIN_ALLOW_THREADS;
        try{
            convert2j(indats, PyBytes_AsString(outdir_py.obj), errors, force);
        }catch(...){
            Py_BLOCK_THREADS;
            throw;
        }
        Py_END_ALLOW_THREADS;

        PyRef errors_py(PyList_New(errors.size()));
        for(size_t i=0; i<errors.size(); i++) {
            auto& err = errors[i];
            PyRef item(PyUnicode_FromString(err.c_str()));
            if(PyList_SetItem(errors_py.obj, i, item.release()))
                return nullptr;
        }

        return errors_py.release();

    }catch(std::exception& e){
        if(PyErr_Occurred())
            return nullptr; // exception already raised

        return PyErr_Format(PyExc_RuntimeError, "Unhandled error: %s", e.what());
    }
}

PyMethodDef methods[] = {
    {"convert2j", (PyCFunction)call_convert2j, METH_VARARGS|METH_KEYWORDS, ""},
    {NULL}
};

struct PyModuleDef engine_convert = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "atf_engine._convert",
    .m_size = 0,
    .m_methods = methods,
};

} // namespace

PyMODINIT_FUNC
PyInit__convert(void)
{
    return PyModuleDef_Init(&engine_convert);
}
