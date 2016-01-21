par(mfrow=c(2, 3))

x <- seq(0, 6, .05)
e <- rnorm(121, 0, .2)
y <- -sin(x) + e
plot(x, y, main="True function and simulated data")
curve(-sin(x), add=TRUE, lwd=4)

color <- c(2, 3, 4, 5, 6) 
color_transparent <- adjustcolor(color, alpha.f = 0.1) 
r <- c()

for (degree in 1:5)
{
  vars <- c()
  biases <- c()
  
  plot(x, -sin(x), type="l", main=paste("True function and degree", degree, "models"), ylab="y")
  curve(-sin(x), add=TRUE, lwd=4)
  for (i in 1:50)
  {
    s <- sample(seq(121), 121, replace=TRUE)
    xs <- x[s]
    ys <- y[s]
    model <- lm(ys ~ poly(xs, degree))
    lines(sort(xs), fitted(model)[order(xs)], col=color_transparent[degree], lwd=2)
    new <- data.frame(xs = seq(0.8, 3.2, 1.2))
    r <- rbind(r, c(degree, predict(model, new)))
    
  }
  
}

par(mfrow=c(1,4))

degree <- 1

plot(x, -sin(x), type="l", main=paste("True function and degree", degree, "models"), ylab="y")
curve(-sin(x), add=TRUE, lwd=4)
for (i in 1:50)
{
  s <- sample(seq(121), 121, replace=TRUE)
  xs <- x[s]
  ys <- y[s]
  model <- lm(ys ~ poly(xs, degree))
  lines(sort(xs), fitted(model)[order(xs)], col=color_transparent[degree], lwd=2)
  new <- data.frame(xs = seq(0.8, 3.2, 1.2))
  r <- rbind(r, c(degree, predict(model, new)))
}
abline(v=seq(0.8, 3.2, 1.2), lty=2)
text(1, 1, "a")
text(2.2, 1, "b")
text(3.4, 1, "c")

for (point in c(3))
{
  points <- r[r[1,]==degree, point]
  plot(density(points), xlim=c(-1.1,0.3), main=paste("Density at", c("a","b","c")[point-1], "( x =", seq(0.8, 3.2, 1.2)[point-1], ")"))
  abline(v=y[x == seq(0.8, 3.2, 1.2)[point-1]], col="blue")
  abline(v=mean(points), col="red")
  abline(v=-sin(seq(0.8, 3.2, 1.2)[point-1]), col="black", lwd=2)
}
