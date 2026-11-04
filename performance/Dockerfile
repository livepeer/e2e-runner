FROM payment-perf-base
# The clean upstream go-livepeer pins LPMS c44af7253bc9, whose installer
# requires this FFmpeg fork commit and its signature-comparison API.
RUN git init /ffmpeg && cd /ffmpeg \
    && git remote add origin https://github.com/livepeer/FFmpeg.git \
    && git fetch --depth=1 origin d9751c73e714b01b363483db358b1ea8022c9bea \
    && git checkout --detach FETCH_HEAD \
    && ./configure --enable-gpl --enable-libx264 --enable-gnutls \
       --enable-shared --disable-static --disable-doc --disable-debug \
    && make -j8 && make install && ldconfig && rm -rf /ffmpeg
LABEL org.livepeer.ffmpeg.commit=d9751c73e714b01b363483db358b1ea8022c9bea
# Force Cgo to recompile against the matching headers if an earlier build
# cached objects against the base image's stock FFmpeg. This unused macro
# changes the build-cache key without changing application behavior.
ENV CGO_CPPFLAGS=-DPAYMENT_PERF_FFMPEG=1
