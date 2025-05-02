'use client';

import { useState, useEffect } from 'react';
import { Post, Comment } from '@/lib/types';
import { likePost, unlikePost, commentOnPost, getPostComments, sharePost } from '@/lib/api';
import { formatDistanceToNow } from 'date-fns';
import { Card, CardContent, CardFooter, CardHeader } from '@/components/ui/card';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';

interface PostCardProps {
  post: Post;
  currentUserId: string;
  onRefresh?: () => void;
  isSharedPost?: boolean;
}

export function PostCard({ post, currentUserId, onRefresh, isSharedPost = false }: PostCardProps) {
  const [liked, setLiked] = useState(post.liked_by_me || false);
  const [likesCount, setLikesCount] = useState(post.likes_count);
  const [commentText, setCommentText] = useState('');
  const [shareText, setShareText] = useState('');
  const [comments, setComments] = useState<Comment[]>([]);
  const [showComments, setShowComments] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [shareDialogOpen, setShareDialogOpen] = useState(false);
  const [originalPost, setOriginalPost] = useState<Post | null>(null);
  const [loadingOriginalPost, setLoadingOriginalPost] = useState(false);

  // Fetch original post if this is a share
  useEffect(() => {
    async function fetchOriginalPost() {
      if (post.post_type === 'SHARE' && post.parent_post_id && !originalPost) {
        try {
          setLoadingOriginalPost(true);
          const response = await fetch(`http://localhost:8000/api/posts/${post.parent_post_id}`);
          if (response.ok) {
            const data = await response.json();
            setOriginalPost(data);
          }
        } catch (error) {
          console.error('Failed to fetch original post:', error);
        } finally {
          setLoadingOriginalPost(false);
        }
      }
    }
    
    fetchOriginalPost();
  }, [post.post_type, post.parent_post_id, originalPost]);

  const handleLike = async () => {
    try {
      if (liked) {
        await unlikePost(currentUserId, post.post_id);
        setLikesCount((prev) => prev - 1);
      } else {
        await likePost(currentUserId, post.post_id);
        setLikesCount((prev) => prev + 1);
      }
      setLiked(!liked);
      if (onRefresh) onRefresh();
    } catch (error) {
      console.error('Failed to like/unlike post:', error);
    }
  };

  const handleComment = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!commentText.trim()) return;
    
    try {
      setIsLoading(true);
      await commentOnPost(currentUserId, post.post_id, commentText);
      setCommentText('');
      if (onRefresh) onRefresh();
      
      // Fetch updated comments
      if (showComments) {
        loadComments();
      }
    } catch (error) {
      console.error('Failed to add comment:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const loadComments = async () => {
    if (!showComments) {
      try {
        setIsLoading(true);
        const postComments = await getPostComments(post.post_id);
        setComments(postComments);
        setShowComments(true);
      } catch (error) {
        console.error('Failed to load comments:', error);
      } finally {
        setIsLoading(false);
      }
    } else {
      setShowComments(false);
    }
  };

  const handleShare = async () => {
    if (!shareText.trim()) return;
    
    try {
      setIsLoading(true);
      await sharePost(currentUserId, post.post_id, shareText);
      setShareText('');
      setShareDialogOpen(false);
      if (onRefresh) onRefresh();
    } catch (error) {
      console.error('Failed to share post:', error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Card className={`mb-4 ${isSharedPost ? 'border-l-4 border-l-blue-300' : ''}`}>
      <CardHeader className="pb-2">
        <div className="flex items-center space-x-3">
          <Avatar className="h-8 w-8">
            <AvatarImage src={post.profile_picture_url || `https://randomuser.me/api/portraits/${post.username?.includes('a') ? 'women' : 'men'}/1.jpg`} alt={post.username || "User"} />
            <AvatarFallback>{(post.username || 'U').substring(0, 1).toUpperCase()}</AvatarFallback>
          </Avatar>
          <div>
            <div className="font-medium">@{post.username || "unknown"}</div>
            <div className="text-xs text-gray-500">
              {formatDistanceToNow(new Date(post.created_at), { addSuffix: true })}
              {post.post_type !== 'TEXT' && (
                <Badge variant="outline" className="ml-2">
                  {post.post_type}
                </Badge>
              )}
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="py-2">
        <p className="whitespace-pre-line">{post.content}</p>
        
        {post.post_type === 'SHARE' && post.parent_post_id && (
          <div className="mt-4 rounded-md border p-3">
            {loadingOriginalPost ? (
              <div className="flex items-center justify-center py-4">
                <div className="h-6 w-6 animate-spin rounded-full border-b-2 border-gray-800"></div>
                <span className="ml-2 text-sm text-gray-600">Loading shared post...</span>
              </div>
            ) : originalPost ? (
              <div className="shared-post">
                <div className="flex items-center space-x-3 mb-2">
                  <Avatar className="h-6 w-6">
                    <AvatarImage src={originalPost.profile_picture_url} />
                    <AvatarFallback>{(originalPost.username || 'U').substring(0, 1).toUpperCase()}</AvatarFallback>
                  </Avatar>
                  <div className="font-medium text-sm">@{originalPost.username || "unknown"}</div>
                </div>
                <p className="whitespace-pre-line text-sm">{originalPost.content}</p>
              </div>
            ) : (
              <div className="py-2 text-sm text-gray-500">
                Original post is no longer available
              </div>
            )}
          </div>
        )}
      </CardContent>
      <CardFooter className="flex flex-col space-y-3 pt-1">
        <div className="flex w-full items-center justify-between">
          <div className="flex space-x-4">
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button 
                    variant="ghost" 
                    size="sm" 
                    onClick={handleLike}
                    className={liked ? "text-red-500" : ""}
                  >
                    {liked ? "♥" : "♡"} {likesCount}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>{liked ? "Unlike" : "Like"}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button 
                    variant="ghost" 
                    size="sm" 
                    onClick={loadComments}
                  >
                    💬 {post.comments_count}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Comments</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            
            {!isSharedPost && (
              <Dialog open={shareDialogOpen} onOpenChange={setShareDialogOpen}>
                <DialogTrigger asChild>
                  <Button variant="ghost" size="sm">
                    ↗ Share
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Share this post</DialogTitle>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="rounded-md bg-gray-100 p-4">
                      <p className="font-medium">@{post.username || "unknown"}</p>
                      <p className="text-sm text-gray-600">{post.content}</p>
                    </div>
                    <Input 
                      placeholder="Add your comment..." 
                      value={shareText}
                      onChange={(e) => setShareText(e.target.value)}
                      disabled={isLoading}
                    />
                    <Button 
                      onClick={handleShare} 
                      disabled={isLoading || !shareText.trim()}
                    >
                      Share Now
                    </Button>
                  </div>
                </DialogContent>
              </Dialog>
            )}
          </div>
        </div>
        
        {showComments && !isSharedPost && (
          <div className="w-full space-y-2">
            <div className="max-h-60 space-y-2 overflow-y-auto rounded-md bg-gray-50 p-2">
              {comments.length > 0 ? (
                comments.map(comment => (
                  <div key={comment.comment_id} className="rounded-md border p-2">
                    <div className="flex items-center space-x-2">
                      <Avatar className="h-6 w-6">
                        <AvatarImage src={comment.profile_picture_url} />
                        <AvatarFallback>{(comment.username || 'U').substring(0, 1).toUpperCase()}</AvatarFallback>
                      </Avatar>
                      <div className="font-medium text-sm">@{comment.username || "unknown"}</div>
                      <div className="text-xs text-gray-500">
                        {formatDistanceToNow(new Date(comment.created_at), { addSuffix: true })}
                      </div>
                    </div>
                    <p className="mt-1 text-sm">{comment.content}</p>
                  </div>
                ))
              ) : (
                <p className="text-center text-sm text-gray-500">No comments yet</p>
              )}
            </div>
            
            <form onSubmit={handleComment} className="flex space-x-2">
              <Input 
                type="text" 
                placeholder="Add a comment..." 
                value={commentText}
                onChange={(e) => setCommentText(e.target.value)}
                disabled={isLoading}
              />
              <Button type="submit" disabled={isLoading || !commentText.trim()}>
                Post
              </Button>
            </form>
          </div>
        )}
      </CardFooter>
    </Card>
  );
} 