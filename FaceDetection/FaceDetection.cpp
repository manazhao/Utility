#include "opencv2/objdetect/objdetect.hpp"
#include "opencv2/highgui/highgui.hpp"
#include "opencv2/imgproc/imgproc.hpp"

#include <iostream>
#include <stdio.h>
#include <string>

using namespace std;
using namespace cv;

/** Function Headers */
size_t detect_face( Mat frame );

/** Global variables */
String face_cascade_name = "haarcascade_frontalface_alt.xml";
String eyes_cascade_name = "haarcascade_eye_tree_eyeglasses.xml";
CascadeClassifier face_cascade;
CascadeClassifier eyes_cascade;
String window_name = "Capture - Face detection";

/** @function main */
int main(int argc, char** argv)
{
	Mat frame;

	//-- 1. Load the cascades
	if( !face_cascade.load( face_cascade_name ) ){ printf("--(!)Error loading face cascade\n"); return -1; };
	if( !eyes_cascade.load( eyes_cascade_name ) ){ printf("--(!)Error loading eyes cascade\n"); return -1; };

	//-- 2. load the image
	string image_path = string(argv[1]);
	frame = imread(image_path,CV_LOAD_IMAGE_COLOR);
	if(!frame.data){
		cout << "could not open or find the image" << endl;
		return 0;
	} 
	size_t num_face = detect_face(frame);
	return num_face;
}

/** @function detectAndDisplay */
size_t detect_face( Mat frame )
{
	std::vector<Rect> faces;
	Mat frame_gray;

	cvtColor( frame, frame_gray, COLOR_BGR2GRAY );
	equalizeHist( frame_gray, frame_gray );

	//-- Detect faces
	face_cascade.detectMultiScale( frame_gray, faces, 1.1, 2, 0|CASCADE_SCALE_IMAGE, Size(30, 30) );
	return faces.size();

}
