exception IllegalArgument {
   1: string message;
}

service MathService {
    double sqrt(1:double num) throws (1: IllegalArgument ia)
    void ping()
}
