FROM scratch
ADD extractor /
CMD ["/extractor","-f","mondial.nt"]