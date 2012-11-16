# ############################################################################
#
#  Simple downloader is to download the resource from fs2you website by the URL of fs2you .
# 
#                                       Author : roc    
#                                       Blog   : http://rocbot.com/  
#                                       Mail   : roc@rocbot.com
#
# ############################################################################

import os
import re
import sys
import time
import copy
import mmap
import base64
import urllib2
import httplib
import urlparse
import threading

def usage( is_cmd = 0):
    if not is_cmd : 
        print ''
        print 'Usage:'
        print '    -h           Help Information'
        print ''
    else :
        print ''
        print 'Command:'
        print ''
        print '    add uri       Add the uri of fs2you to download'
        print '    del uri       Delete the uri of fs2you if exist'
        print ''
        print '   dump           Dump channels status'
        print '   quit           Quit simple downloader'
        print ''
    return

class HTTP_Handler (object):
    """
    Simple HTTP Handler : to recv http response headers and data
    """
    def __init__(self):
        self._clear()
        return
    def _clear(self):
        self._status = None
        self._reason = None
        self._headers = None
        self._response = None
        return
    def get(self, uri, headers):
        self._clear()
        content = ''
        try:
            h,hostname,path,query,a,b = urlparse.urlparse(uri)
            conn = httplib.HTTPConnection(hostname)
            conn.request("GET", path, query, headers)
            self._response = conn.getresponse()
            self._status = self._response.status
            self._reason = self._response.reason
            self._headers = self._response.getheaders()
            content = self._response.read()
            conn.close()
        except:
            pass
        return content
    def status(self):
        return self._status
    def reason(self):
        return self._reason
    def headers(self):
        return self._headers
    def response(self):
        return self._response
    
class Bitmap_Manager (object):
    """
    Simple Bitmap Manager : to manage the bitmap of download file
    """
    def __init__(self):
        self._exit = 0
        self._block_size = 262144 # default block size
        self._file_size = -1
        self._bitmap = {}
        self._unfilled = []
        self._lock = threading.Lock()
        self._callback = None
        self._channel_id = None
        self._check_interval = 5
        self._pending_timeout = 60
        self._is_ready = 0
        self._filled_count = 0
        return
    def open(self, callback, file_size, channel_id):
        # check params
        if file_size == 0 : return 0
        # record params
        self._callback = callback
        self._file_size = file_size
        self._channel_id = channel_id
        # calculate block count
        if self._file_size % self._block_size == 0 :
            self._block_count = int(self._file_size / self._block_size)
        else :
            self._block_count = int(self._file_size / self._block_size) + 1
        if not self._recover():
            # init bitmap
            self._lock.acquire()
            for n in range(0, self._block_count):
                if n == self._block_count-1 : # for last block
                    block_size = self._file_size - n*self._block_size
                else : # for normal block
                    block_size = self._block_size
                self._bitmap[n] = [0, (n*self._block_size, n*self._block_size+block_size, block_size), time.time()] # bitmap status 0, 1, 2 for unfilled, filled, pending
                self._unfilled.append(n)
                #print n, '%d %d %d' % (n*self._block_size, n*self._block_size+block_size, block_size)
            self._lock.release()
        else :
            self._unfilled.sort()
        # start check thread
        h = threading.Thread(target=self._worker, args=())
        h.start()
        return 1
    def close(self):
        self._backup()
        self._exit = 1
        return
    def generate(self):
        block_numb = None
        block_range = None
        self._lock.acquire()
        if len(self._unfilled) > 0:
            block_numb = self._unfilled[0]
            block_range = self._bitmap[block_numb][1]
            self._bitmap[block_numb][0] = 2
            self._bitmap[block_numb][2] = time.time()
            del self._unfilled[0]
        self._lock.release()
        return block_numb, block_range
    def update(self, block_numb, is_filled):
        if block_numb >= self._block_count : return 0
        self._lock.acquire()
        if is_filled == 1: # filled
            self._bitmap[block_numb][0] = 1
        elif is_filled == 0: # unfilled
            self._bitmap[block_numb][0] = 0
            self._unfilled.append(block_numb)
        self._lock.release()
        return 1
    def _dump(self):
        rc = str(self._channel_id)+' '+str(self._filled_count*100/self._block_count)+'% '
        self._lock.acquire()
        for n in range(0, self._block_count):
            rc += str(self._bitmap[n][0])
        self._lock.release()
        print rc
        return
    def _worker(self):
        last_check_time = time.time()
        while not self._exit:
            if time.time() - last_check_time > self._check_interval:
                last_check_time = time.time()
                if not self._is_ready :
                    self._check()
                    self._backup()
                    self._dump()
            time.sleep(1)
        return
    def _check(self):
        is_ready = 1
        pending_blocks = []
        # make a copy
        self._lock.acquire()
        bitmap = copy.copy(self._bitmap)
        self._lock.release()
        # check ready 
        if len(self._unfilled) == 0:
            for k in bitmap.keys():
                if bitmap[k][0] != 1:
                    is_ready = 0
                    break
        else:
            is_ready = 0
        # chenck pending block
        self._filled_count = 0
        for k in bitmap.keys():
            if bitmap[k][0] == 2 and time.time() - bitmap[k][2] > self._pending_timeout:
                pending_blocks.append(k)
            if bitmap[k][0] == 1:
                self._filled_count += 1
        # if ready , callback
        if is_ready :
            self._is_ready = 1
            self._callback.ready()
        # prcoess pending_blocks
        self._lock.acquire()
        for block in pending_blocks:
            self._unfilled.append(block)
            self._bitmap[block][0] = 0
            self._bitmap[block][2] = time.time()
        self._lock.release()
        # sort unfilled
        self._unfilled.sort()
        return
    def _recover(self):
        rc = 0
        # read backup file
        try:
            f = open(str(self._channel_id)+'.bak','r+')
            bitmap = eval(f.readline())
            unfilled = eval(f.readline())
            f.close()
            self._lock.acquire()
            self._bitmap = bitmap
            self._unfilled = unfilled
            self._lock.release()
            rc = 1
        except:
            pass
        return rc
    def _backup(self):
        # make copy
        self._lock.acquire()
        bitmap = copy.copy(self._bitmap)
        unfilled = copy.copy(self._unfilled)
        self._lock.release()
        # save backup file
        try:
            f = open(str(self._channel_id)+'.bak','w+')
            f.write(str(bitmap)+'\n')
            f.write(str(unfilled)+'\n')
            f.close()
        except:
            pass
        return

class Cache_Mangaer (object):
    """
    Simple Cache Manager : to manage the cache of download file
    """
    def __init__(self):
        self._file_name = None
        self._channel_id = None
        self._file_size = None
        self._file_mmap = None
        self._file = None
        self._cache_name = None
        self._lock = threading.Lock()
        return
    def open(self, file_size, channel_id, file_name):
        # check file_size
        if file_size == 0 : return 0
        if os.path.exists(file_name) : return 0
        # record params
        self._channel_id = channel_id
        self._file_name = file_name
	if self._file_name=="":self._file_name=str(channel_id)
        self._file_size = file_size
        # create file mmap
        self._cache_name = str(channel_id)+'.dat'
        if not os.path.exists(self._cache_name) :
            self._file = open(self._cache_name, 'wb+')
        else:
            self._file = open(self._cache_name, 'rb+')
            
        return 1
    def close(self):
        #if self._file_mmap != None :
        #    self._file_mmap.close()
        #    self._file_mmap = None
        if self._file != None :
            self._file.close()
            self._file = None
        return
    def push(self, data, offset):
        #print 'push', len(data), offset
        #if self._file_mmap == None : return 0
        if self._file == None : return 0
        rc = 1
        self._lock.acquire()
        try:
            #self._file_mmap = mmap.mmap(self._file.fileno(), self._file_size)
            #self._file_mmap.seek(offset)
            #self._file_mmap.write(data)
            #self._file_mmap.flush()
            #self._file_mmap.close()
		
	    self._file.seek(offset)
	    self._file.write(data)
	    self._file.flush()
        except Exception,e:
	    print e
            pass
            rc = 0
        self._lock.release()
        return rc
    def finish(self):
        self.close()
        try:
            os.rename(self._cache_name, self._file_name)
        except Exception,e:
	    print e
            pass
            print 'ERROR: can not rename', self._cache_name, 'to', self._file_name
        return
    
class Channel (object):
    """
    Simple Channel : to manage one download file channel
    """
    def __init__(self, uri):
        self._exit = 0
        self._status = -1
        self._callback = None
        self._bitmap_mgr = None
        self._cache_mgr = None
        self._uri = uri
        self._title = ''
        self._tsize = ''
        self._dtime = ''
        self._descr = ''
        self._ulink = ''
        self._filesize = -1
        self._worker_numb = 5 # default worker thread number
        self._channel_id = None
        return
    def open(self, callback):
        print 'open channel:', self._uri
        self._channel_id = hash(self._uri)
        # record callback
        self._callback = callback
        # get real link
        page = self._get_page()
        if page == '' : return 0
        self._title, self._tsize, self._dtime, self._descr, self._ulink = self._parse(page)
        if self._ulink == '' : return 0
        print 'get real link:', self._ulink
        # get file size
        self._file_size = self._get_file_size(self._ulink)
        if self._file_size == 0 : return 0
        print 'get file size:', self._file_size
        #create cache manager
        self._cache_mgr = Cache_Mangaer()
        if not self._cache_mgr.open(self._file_size, self._channel_id, self._title) : return 0
        print 'create cache: ok'
        # create bitmap manager
        self._bitmap_mgr = Bitmap_Manager()
        if not self._bitmap_mgr.open(self, self._file_size, self._channel_id) : return 0
        print 'create bitmap: ok'
        # create workers
        for n in range(0, self._worker_numb):
            h = threading.Thread(target=self._worker, args=(self._ulink,))
            h.start()
        print 'start workers: ok'
        # change self status
        self._status = 0
        return 1
    def close(self):
        self._exit = 1
        self._status = -1
        if self._bitmap_mgr != None :
            self._bitmap_mgr.close()
        if self._cache_mgr != None :
            self._cache_mgr.close()
        return
    def status(self):
        return self._status
    def uri(self):
        return self._uri
    def ready(self):
        """
        callback for bitmap manager
        """
        print self._uri, 'is ready'
        self._cache_mgr.finish()
        self._callback.ready(self._channel_id)
        return
    def _get_page(self):
        handle = HTTP_Handler()
        headers = {}
        headers['User-Agent'] = 'FireFox 3.0'
        #headers['Host'] = urlparse.urlparse(self._uri).hostname
        headers['Host'] = urlparse.urlparse(self._uri)[1]
        headers['Accept'] = '*/*'
        page = handle.get(self._uri, headers)
        if (handle.status() == 301 or handle.status() == 302) and handle.response() != None:
            location = handle.response().getheader('Location')
            page = handle.get(location, headers)
        return page
    def _get_file_size(self, uri):
        file_size = 0
        handle = HTTP_Handler()
        headers = {}
        headers['User-Agent'] = 'Grid Service 2.0'
        #headers['Grid'] =  base64.encodestring('id=hehe&p_num=0&d_speed=262144')
        headers['Range'] = 'bytes=0-0'
        #headers['Host'] = urlparse.urlparse(uri).hostname
        headers['Host'] = urlparse.urlparse(uri)[1]
        headers['Accept'] = '*/*'
        page = handle.get(uri, headers)
	print handle.status()
        if handle.response() != None:
            content_range = handle.response().getheader('Content-Range')
            if content_range != None :
                file_size = int(content_range[content_range.rfind('/')+1:])
        return file_size
    def _worker(self, uri):
        handle = HTTP_Handler()
        while not self._exit:
            block_numb, block_range = self._bitmap_mgr.generate()
            if block_numb != None and block_range != None:
                headers = {}
                headers['User-Agent'] = 'Grid Service 2.0'
                #headers['Grid'] =  base64.encodestring('id=haha&p_num=0&d_speed=262144')
                headers['Range'] = 'bytes='+'%d-%d' % (block_range[0], block_range[1])
                #headers['Host'] = urlparse.urlparse(uri).hostname
                headers['Host'] = urlparse.urlparse(uri)[1]
                headers['Accept'] = '*/*'
                headers['Connection'] = 'close'
                data = handle.get(uri, headers)
                #print block_numb, block_range, len(data), handle.status(), handle.reason()
                if handle.status() == 200 or handle.status() == 206: 
                    if self._cache_mgr.push(data, block_range[0]) :  #save data to cache
                        self._bitmap_mgr.update(block_numb, 1) #update bitmap filled
                else :
                    self._bitmap_mgr.update(block_numb, 0) #update bitmap unfilled
                data = None
            time.sleep(0.01)
        return
    def _parse(self, page):
        title = self._find_sub(page, '<h1 id="fileNameTitle" class="fileName">', '</h1>')
        tsize = self._find_sub(page, '<span>Size: </span><span>', '</span></li>')
        dtime = self._find_sub(page, '<span>Publish date: </span><span>', '</span></li>')
        descr = self._find_sub(page, '<div class="panel-con"><p><p>', '</p></p></div>')
        ulink = self._find_sub(page, 'var url = \'', '\';')
        return title, tsize, dtime, descr, ulink
    def _find_sub(self, src, begin, end):
        r = ''
        try:
            t_b = src.find(begin);
            if t_b == -1: return r
            t_b = t_b + len(begin)
            t_e = src.find(end, t_b)
            if t_e == -1: return r
            r = src[t_b:t_e]
        except:
            pass
        return r
class Channel_Manager (object):
    """
    Simple Channel Manager : to manage channels
    """
    def __init__(self):
        self._channels = {}
        self._channels_lock = None
        self._active_channels = {}
        self._active_channels_lock = None
        self._max_active_channel = 5
        self._exit = 0
        self._check_interval = 3
        return
    def open(self):
        # create locks
        self._channels_lock = threading.Lock()
        self._active_channels_lock = threading.Lock()
        # start worker
        h = threading.Thread(target=self._worker, args=())
        h.start()
        return 1
    def close(self):
        self._exit = 1
        self._channels_lock.acquire()
        for key in self._channels.keys():
            channel = self._channels[key]
            channel.close()
        self._channels_lock.release()
        return
    def open_channel(self, uri):
        key = hash(uri)
        # check channel
        if self._is_in_channels(key):
            return 0
        # create channel
        channel = Channel(uri)
        # add to channels
        self._add_to_channels(key, channel)
        return 1
    def close_channel(self, uri):
        key = hash(uri)
        #check channel
        if not self._is_in_channels(key):
            return 1
        self._channels_lock.acquire()
        channel = self._channels[key]
        channel.close()
        del self._channels[key]
        self._channels_lock.release()
        return
    def count(self):
        self._channels_lock.acquire()
        c = len(self._channels)
        self._channels_lock.release()
        return c
    def ready(self, channel_id):
        return
    def dump(self):
        self._channels_lock.acquire()
        for key in self._channels.keys():
            channel = self._channels[key]
            print key, channel.status(), channel.uri()
        self._channels_lock.release()
        return
    def _is_in_channels(self, key):
        rc = 0
        self._channels_lock.acquire()
        if self._channels.has_key(key):
            rc = 1
        self._channels_lock.release()
        return rc
    def _add_to_channels(self, key, channel):
        self._channels_lock.acquire()
        self._channels[key] = channel
        self._channels_lock.release()
        return
    def _add_to_active_channels(self, key, channel):
        self._active_channels_lock.acquire()
        self._active_channels[key] = channel
        self._active_channels_lock.release()
        return
    def _get_active_channel_count(self):
        self._active_channels_lock.acquire()
        c = len(self._active_channels)
        self._active_channels_lock.release()
        return c
    def _worker(self):
        last_check_time = time.time()
        while not self._exit:
            if time.time() - last_check_time > self._check_interval:
                last_check_time = time.time()
                self._check()
            time.sleep(1)
        return
    def _check(self):
        # check max active channel
        if self._get_active_channel_count() >= self._max_active_channel:
            return
        # get a pending channel
        key = None
        channel = None
        self._channels_lock.acquire()
        for k in self._channels.keys():
            ch = self._channels[k]
            if ch.status() == -1:
                key = k
                channel = ch
                break
        self._channels_lock.release()
        # try to open channel
        if channel != None and key != None:
            if channel.open(self):
                self._add_to_active_channels(key, channel)
        return

class Simple_Downloader (object):
    """
    Simple Downloader : to manage download files with console
    """
    def __init__(self):
        self._version = '1.0.0.0'
        self._exit = 0
        self._channel_mgr = None
        return
    def open(self):
        # create and open channel_manager
        self._channel_mgr = Channel_Manager()
        if not self._channel_mgr.open() :
            return 0
        return 1
    def close(self):
        # close channel_manager
        self._channel_mgr.close()
        return
    def process(self):
        while not self._exit:
            input = raw_input('>>\n')
            ss = input.split(' ', 1)
            if len(ss) == 1:
                cmd = ss[0].upper()
                if cmd == 'QUIT':
                    self._exit = 1
                    print 'program is quiting, please waiting...'
                elif cmd == 'DUMP':
                    self._channel_mgr.dump()
                else:
                    usage(1)
            elif len(ss) == 2:
                cmd = ss[0].upper()
                value = ss[1]
                if cmd == 'ADD':
                    self._channel_mgr.open_channel(value)
                elif cmd == 'DEL':
                    self._channel_mgr.close_channel(value)
                else:
                    usage(1)
            else:
                usage(1)
        return
    
 
if __name__ == '__main__':
    
    # create downloader
    dl = Simple_Downloader()
    
    # open downloader
    if not dl.open() : 
        usage(0)
        exit(-1)
    
    # process
    dl.process()
    
    # close downloader
    dl.close()
    
