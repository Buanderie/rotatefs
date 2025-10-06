/** @file
 *
 * Compile with:
 *
 *     gcc -Wall rotatefs.c `pkg-config fuse --cflags --libs` -lulockmgr -o rotatefs
 *
 */

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 29

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE
#define _XOPEN_SOURCE 600

#include <fuse.h>

#ifdef HAVE_LIBULOCKMGR
#include <ulockmgr.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

#include <ftw.h>
#include <limits.h>
#include <sys/types.h>
#include <libgen.h>
#include <ctype.h> /* for tolower */

struct rfs_state {
    char *rootdir;
    char oldest_path[PATH_MAX];
    int files_traversed;
    time_t oldest_mtime;
    size_t directory_usage;
};
#define RFS_DATA ((struct rfs_state *) fuse_get_context()->private_data)

static struct options {
    size_t max_device_size;
} options;

static int parse_size(const char *str, size_t *result) {
    char *endptr;
    unsigned long long num = strtoull(str, &endptr, 10);
    if (num == ULLONG_MAX && errno == ERANGE) {
        return -1;
    }
    if (endptr == str) {
        return -1;
    }
    size_t mult = 1;
    if (*endptr != '\0') {
        char unit = tolower(*endptr);
        endptr++;
        switch (unit) {
            case 'k':
                mult = 1024;
                break;
            case 'm':
                mult = 1024UL * 1024;
                break;
            case 'g':
                mult = 1024UL * 1024 * 1024;
                break;
            default:
                return -1;
        }
        // Skip any trailing whitespace or ensure no extra chars
        while (isspace(*endptr)) endptr++;
        if (*endptr != '\0') {
            return -1;
        }
    }
    *result = (size_t)num * mult;
    return 0;
}

int save_older (const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    if (typeflag != FTW_F) return 0;

    if (RFS_DATA->files_traversed == 0 || sb->st_mtime < RFS_DATA->oldest_mtime) {
        strcpy(RFS_DATA->oldest_path, fpath);
        RFS_DATA->oldest_mtime = sb->st_mtime;
        RFS_DATA->files_traversed++;
    } else {
        RFS_DATA->files_traversed++;
    }

    return 0;
}

int rm_empty_dirs(const char *fpath)
{
    char path_copy[PATH_MAX];
    char *dirpath;
    int res = 0;

    strcpy(path_copy, fpath);
    dirpath = path_copy;

    do {
        dirpath = dirname(dirpath);

        /* dirname("")    == "." (error)
         * dirname("/")   == "/" (ok)
         * dirname(".")   == "." (error)
         * dirname("..")  == "." (error)
         * dirname("a/b") == "a" (error)
         * So, assuming the path received by this function always start with a
         * '/', because it expects a full path. If the dirname have a lenght of
         * 1 there is no more directories to remove, or an invalid path was
         * given.
         */
        switch (strnlen(dirpath, 2)) {
            case 0:
                errno = EINVAL;
                return -errno;
            case 1:
                if (dirpath[0] == '/')
                    return 0;
                else {
                    errno = EINVAL;
                    return -errno;
                }
        }
        res = rmdir(dirpath);
    } while(res == 0);

    return (errno == ENOTEMPTY) ? 0 : -errno;
}

int delete_oldest()
{
    int res;

    RFS_DATA->files_traversed = 0;
    RFS_DATA->oldest_mtime = 0;

    if (nftw(RFS_DATA->rootdir, save_older, FOPEN_MAX, FTW_MOUNT | FTW_PHYS) != 0) {
        perror("error occurred: ");
        return -errno;
    }

    if (RFS_DATA->files_traversed == 0) {
        return 0;  // No files to delete
    }

    res = unlink(RFS_DATA->oldest_path);
    if (res == -1)
        return -errno;

    /* after the file deleted, will need to search for the oldest file again. */
    RFS_DATA->files_traversed = 0;

    return rm_empty_dirs(RFS_DATA->oldest_path);
}

int sum(const char *fpath, const struct stat *sb, int typeflag) {
    if (typeflag == FTW_F) {
        RFS_DATA->directory_usage += sb->st_size;
    }
    return 0;
}

size_t current_size(char *rootdir)
{
    RFS_DATA->directory_usage = 0;

    if (ftw(rootdir, &sum, 1))
        return (size_t)-errno;
    else
        return RFS_DATA->directory_usage;
}

size_t underlying_device_size(char *rootdir)
{
    int res;
    size_t fsize;
    struct statvfs *stbuf = malloc(sizeof(struct statvfs));

    res = statvfs(rootdir, stbuf);
    fsize = stbuf->f_bsize * stbuf->f_blocks;
    free(stbuf);
    if (res == -1) {
        return (size_t)-errno;
    }

    return fsize;
}

void trim_fs(size_t added)
{
    size_t cap = options.max_device_size;
    if (cap == 0) {
        return;  // No limit
    }
    size_t target = cap > added ? cap - added : 0;
    while (current_size(RFS_DATA->rootdir) > target) {
        fprintf(stderr, "Trimming: current %zu > target %zu (cap %zu, added %zu)\n",
                current_size(RFS_DATA->rootdir), target, cap, added);
        if (delete_oldest() != 0) {
            break;
        }
    }
}

//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, RFS_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
				    // break here
}

static void *rfs_init(struct fuse_conn_info *conn)
{
    (void) conn;

    // trim filesystem to the max allowed size on initialization
    trim_fs(0);

    return RFS_DATA;
}

static int rfs_getattr(const char *path, struct stat *stbuf)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = lstat(fpath, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_fgetattr(const char *path, struct stat *stbuf,
			struct fuse_file_info *fi)
{
	int res;

	(void) path;

	res = fstat(fi->fh, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_access(const char *path, int mask)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = access(fpath, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_readlink(const char *path, char *buf, size_t size)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = readlink(fpath, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}

struct rfs_dirp {
	DIR *dp;
	struct dirent *entry;
	off_t offset;
};

static int rfs_opendir(const char *path, struct fuse_file_info *fi)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	struct rfs_dirp *d = malloc(sizeof(struct rfs_dirp));
	if (d == NULL)
		return -ENOMEM;

	d->dp = opendir(fpath);
	if (d->dp == NULL) {
		res = -errno;
		free(d);
		return res;
	}
	d->offset = 0;
	d->entry = NULL;

	fi->fh = (unsigned long) d;
	return 0;
}

static inline struct rfs_dirp *get_dirp(struct fuse_file_info *fi)
{
	return (struct rfs_dirp *) (uintptr_t) fi->fh;
}

static int rfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	struct rfs_dirp *d = get_dirp(fi);

	(void) path;
	if (offset != d->offset) {
		seekdir(d->dp, offset);
		d->entry = NULL;
		d->offset = offset;
	}
	while (1) {
		struct stat st;
		off_t nextoff;

		if (!d->entry) {
			d->entry = readdir(d->dp);
			if (!d->entry)
				break;
		}

		memset(&st, 0, sizeof(st));
		st.st_ino = d->entry->d_ino;
		st.st_mode = d->entry->d_type << 12;
		nextoff = telldir(d->dp);
		if (filler(buf, d->entry->d_name, &st, nextoff))
			break;

		d->entry = NULL;
		d->offset = nextoff;
	}

	return 0;
}

static int rfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	struct rfs_dirp *d = get_dirp(fi);
	(void) path;
	closedir(d->dp);
	free(d);
	return 0;
}

static int rfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	if (S_ISFIFO(mode))
		res = mkfifo(fpath, mode);
	else
		res = mknod(fpath, mode, rdev);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_mkdir(const char *path, mode_t mode)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = mkdir(fpath, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_unlink(const char *path)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = unlink(fpath);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_rmdir(const char *path)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = rmdir(fpath);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_symlink(const char *from, const char *to)
{
	int res;
        char ffrom[PATH_MAX];
        char fto[PATH_MAX];

        fullpath(ffrom, from);
        fullpath(fto, to);
	res = symlink(ffrom, fto);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_rename(const char *from, const char *to)
{
	int res;
        char ffrom[PATH_MAX];
        char fto[PATH_MAX];

        fullpath(ffrom, from);
        fullpath(fto, to);
	res = rename(ffrom, fto);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_link(const char *from, const char *to)
{
	int res;
        char ffrom[PATH_MAX];
        char fto[PATH_MAX];

        fullpath(ffrom, from);
        fullpath(fto, to);
	res = link(ffrom, fto);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_chmod(const char *path, mode_t mode)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
        res = chmod(fpath, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
        res = lchown(fpath, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_truncate(const char *path, off_t size)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);

        // Proactive trim if growing the file
        struct stat st;
        off_t old_size = 0;
        if (lstat(fpath, &st) == 0 && S_ISREG(st.st_mode)) {
            old_size = st.st_size;
            if (size > old_size) {
                trim_fs((size_t)(size - old_size));
            }
        }

	res = truncate(fpath, size);
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_ftruncate(const char *path, off_t size,
			 struct fuse_file_info *fi)
{
	int res;

	(void) path;

        // Proactive trim if growing (but hard without old size; conservative)
        struct stat st;
        off_t old_size = 0;
        if (fstat(fi->fh, &st) == 0 && S_ISREG(st.st_mode)) {
            old_size = st.st_size;
            if (size > old_size) {
                trim_fs((size_t)(size - old_size));
            }
        }

	res = ftruncate(fi->fh, size);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_UTIMENSAT
static int rfs_utimens(const char *path, const struct timespec ts[2],
		       struct fuse_file_info *fi)
{
	int res;
        char fpath[PATH_MAX];

	/* don't use utime/utimes since they follow symlinks */
	if (fi)
            res = futimens(fi->fh, ts);
        else {
            fullpath(fpath, path);
            res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
        }
	if (res == -1)
            return -errno;

	return 0;
}
#endif

static int rfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int fd;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	fd = open(fpath, fi->flags, mode);
	if (fd == -1)
		return -errno;

	fi->fh = fd;
	return 0;
}

static int rfs_open(const char *path, struct fuse_file_info *fi)
{
	int fd;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	fd = open(fpath, fi->flags);
	if (fd == -1)
		return -errno;

	fi->fh = fd;
	return 0;
}

static int rfs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res;

	(void) path;
	res = pread(fi->fh, buf, size, offset);
	if (res == -1)
		res = -errno;

	return res;
}

static int rfs_read_buf(const char *path, struct fuse_bufvec **bufp,
			size_t size, off_t offset, struct fuse_file_info *fi)
{
	struct fuse_bufvec *src;

	(void) path;

	src = malloc(sizeof(struct fuse_bufvec));
	if (src == NULL)
		return -ENOMEM;

	*src = FUSE_BUFVEC_INIT(size);

	src->buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
	src->buf[0].fd = fi->fh;
	src->buf[0].pos = offset;

	*bufp = src;

	return 0;
}

static int rfs_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int res;

	(void) path;

    trim_fs(size);

    size_t cap = options.max_device_size;
    res = pwrite(fi->fh, buf, size, offset);
    while (res == -1 && errno == ENOSPC) {
        if (cap > 0 && cap < size) {
            break;  // Can't make space for this large write
        }
        fprintf(stderr, "Fallback trim: cap %zu, write size %zu\n", cap, size);
        if (delete_oldest() != 0) {
            break;
        }
        res = pwrite(fi->fh, buf, size, offset);
    }
	
	if (res == -1)
		res = -errno;

	return res;
}

static int rfs_write_buf(const char *path, struct fuse_bufvec *buf,
		     off_t offset, struct fuse_file_info *fi)
{
	struct fuse_bufvec dst = FUSE_BUFVEC_INIT(fuse_buf_size(buf));
    int res;

	(void) path;

	dst.buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
	dst.buf[0].fd = fi->fh;
	dst.buf[0].pos = offset;

    size_t bufsize = fuse_buf_size(buf);
    trim_fs(bufsize);

    size_t cap = options.max_device_size;
    res = fuse_buf_copy(&dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
    while (res == -ENOSPC) {
        if (cap > 0 && cap < bufsize) {
            break;
        }
        fprintf(stderr, "Fallback trim: cap %zu, buf size %zu\n", cap, bufsize);
        if (delete_oldest() != 0) {
            break;
        }
        res = fuse_buf_copy(&dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
    }

	return res;
}

static int rfs_statfs(const char *path, struct statvfs *stbuf)
{
	int res;
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	res = statvfs(fpath, stbuf);
	if (res == -1)
		return -errno;

    if (options.max_device_size > 0) {
        size_t cap = options.max_device_size;
        size_t used = current_size(RFS_DATA->rootdir);
        stbuf->f_blocks = cap / stbuf->f_bsize;
        stbuf->f_bfree = stbuf->f_bavail = (cap > used ? (cap - used) / stbuf->f_bsize : 0);
        // Arbitrary large numbers for inodes
        stbuf->f_files = 100000;
        stbuf->f_ffree = 99999;
    }

	return 0;
}

static int rfs_flush(const char *path, struct fuse_file_info *fi)
{
	int res;

	(void) path;
	/* This is called from every close on an open file, so call the
	   close on the underlying filesystem.	But since flush may be
	   called multiple times for an open file, this must not really
	   close the file.  This is important if used on a network
	   filesystem like NFS which flush the data/metadata on close() */
	res = close(dup(fi->fh));
	if (res == -1)
		return -errno;

	return 0;
}

static int rfs_release(const char *path, struct fuse_file_info *fi)
{
	(void) path;
	close(fi->fh);

	return 0;
}

static int rfs_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	int res;
	(void) path;

#ifndef HAVE_FDATASYNC
	(void) isdatasync;
#else
	if (isdatasync)
		res = fdatasync(fi->fh);
	else
#endif
		res = fsync(fi->fh);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int rfs_fallocate(const char *path, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
	(void) path;

	if (mode)
		return -EOPNOTSUPP;

	return -posix_fallocate(fi->fh, offset, length);
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int rfs_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	int res = lsetxattr(fpath, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int rfs_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	int res = lgetxattr(fpath, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int rfs_listxattr(const char *path, char *list, size_t size)
{
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	int res = llistxattr(fpath, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int rfs_removexattr(const char *path, const char *name)
{
        char fpath[PATH_MAX];

        fullpath(fpath, path);
	int res = lremovexattr(fpath, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

#ifdef HAVE_LIBULOCKMGR
static int rfs_lock(const char *path, struct fuse_file_info *fi, int cmd,
		    struct flock *lock)
{
	(void) path;

	return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner,
			   sizeof(fi->lock_owner));
}
#endif

static int rfs_flock(const char *path, struct fuse_file_info *fi, int op)
{
	int res;
	(void) path;

	res = flock(fi->fh, op);
	if (res == -1)
		return -errno;

	return 0;
}

static struct fuse_operations rfs_oper = {
	.init           = rfs_init,
	.getattr	= rfs_getattr,
	.fgetattr	= rfs_fgetattr,
	.access		= rfs_access,
	.readlink	= rfs_readlink,
	.opendir	= rfs_opendir,
	.readdir	= rfs_readdir,
	.releasedir	= rfs_releasedir,
	.mknod		= rfs_mknod,
	.mkdir		= rfs_mkdir,
	.symlink	= rfs_symlink,
	.unlink		= rfs_unlink,
	.rmdir		= rfs_rmdir,
	.rename		= rfs_rename,
	.link		= rfs_link,
	.chmod		= rfs_chmod,
	.chown		= rfs_chown,
	.truncate	= rfs_truncate,
	.ftruncate	= rfs_ftruncate,
#ifdef HAVE_UTIMENSAT
	.utimens	= rfs_utimens,
#endif
	.create		= rfs_create,
	.open		= rfs_open,
	.read		= rfs_read,
	.read_buf	= rfs_read_buf,
	.write		= rfs_write,
	.write_buf	= rfs_write_buf,
	.statfs		= rfs_statfs,
	.flush		= rfs_flush,
	.release	= rfs_release,
	.fsync		= rfs_fsync,
#ifdef HAVE_POSIX_FALLOCATE
	.fallocate	= rfs_fallocate,
#endif
#ifdef HAVE_SETXATTR
	.setxattr	= rfs_setxattr,
	.getxattr	= rfs_getxattr,
	.listxattr	= rfs_listxattr,
	.removexattr	= rfs_removexattr,
#endif
#ifdef HAVE_LIBULOCKMGR
	.lock		= rfs_lock,
#endif
	.flock		= rfs_flock,
};

void rfs_usage()
{
    fprintf(stderr, "usage:  rotatefs [FUSE and mount options] rootDir mountPoint [-s <fs_size>|--size=<fs_size>]\n");
    abort();
}

int main(int argc, char *argv[])
{
    int fuse_stat, i;
    struct rfs_state *rfs_data;

    umask(0);

    // See which version of fuse we're running
    fprintf(stderr, "Fuse library version %d.%d\n", FUSE_MAJOR_VERSION, FUSE_MINOR_VERSION);

    // Perform some sanity checking on the command line:  make sure
    // there are enough arguments, and that neither of the last two
    // start with a hyphen (this will break if you actually have a
    // rootpoint or mountpoint whose name starts with a hyphen, but so
    // will a zillion other programs)
    if ((argc < 3)) {
        for (i = 0; i < argc; i++)
            fprintf(stderr, "argv[%d]: %s\n", i, argv[i]);
        rfs_usage();
    }

    rfs_data = malloc(sizeof(struct rfs_state));
    if (rfs_data == NULL) {
	perror("main calloc");
	abort();
    }
    // Pull the rootdir out of the argument list and save it in my
    // internal data
    rfs_data->rootdir = realpath(argv[1], NULL);
    for (i = 2; i < argc; i++) {
        argv[i-1] = argv[i];
    }
    argv[argc-1] = NULL;
    argc--;
    fprintf(stderr, "rootdir: %s\n", rfs_data->rootdir);

    options.max_device_size = 0;    // default value

    // Manually parse and remove size options
    for (i = 1; i < argc; ) {  // Start from 1 (prog name at 0), increment manually
        if (strcmp(argv[i], "-s") == 0) {
            i++;
            if (i >= argc) {
                rfs_usage();
            }
            if (parse_size(argv[i], &options.max_device_size) != 0) {
                fprintf(stderr, "Invalid size value: %s\n", argv[i]);
                rfs_usage();
            }
            // Remove -s and value
            memmove(&argv[i-1], &argv[i+1], sizeof(char *) * (argc - i));
            argc -= 2;
            // Do not increment i, as contents shifted
            continue;
        } else if (strncmp(argv[i], "--size=", 7) == 0) {
            if (parse_size(argv[i] + 7, &options.max_device_size) != 0) {
                fprintf(stderr, "Invalid size value: %s\n", argv[i] + 7);
                rfs_usage();
            }
            // Remove the --size=... arg
            memmove(&argv[i], &argv[i+1], sizeof(char *) * (argc - i));
            argc -= 1;
            // Do not increment i
            continue;
        }
        i++;
    }

    fprintf(stderr, "options.max_device_size: %zu\n", options.max_device_size);

    rfs_data->files_traversed = 0;

    // turn over control to fuse
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    fuse_stat = fuse_main(args.argc, args.argv, &rfs_oper, rfs_data);
    
    return fuse_stat;
}